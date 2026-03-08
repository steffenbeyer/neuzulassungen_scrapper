"""
OpenEV Data Importer: Importiert E-Fahrzeug-Spezifikationen aus dem
OpenEV-Data-Projekt (https://github.com/open-ev-data).

Datenquelle: OpenEV Data Dataset (AGPL-3.0 Lizenz)
Format: CSV mit ~1200 E-Fahrzeug-Eintraegen

Importiert werden:
- Batteriekapazitaet (brutto/netto kWh)
- Reichweite (WLTP/EPA km)
- Leistung (kW/PS)
- Antrieb (FWD/RWD/AWD)
- DC/AC Ladeleistung (kW)
- Beschleunigung 0-100 km/h
- Hoechstgeschwindigkeit

Die Daten werden als modell_varianten gespeichert und bestehenden
Modellen in der DB zugeordnet (kein Erstellen neuer Modelle).

Usage:
    python main.py --mode import-openev
"""
import csv
import logging
import os
import re
import requests
from pathlib import Path

from config import Config
from src.database import db
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)

# Download-URL fuer die neueste OpenEV CSV-Datei
OPENEV_RELEASES_API = 'https://api.github.com/repos/open-ev-data/open-ev-data-dataset/releases/latest'
OPENEV_CSV_PATH = os.path.join(Config.DOWNLOAD_DIR, '..', 'openev', 'openev-data.csv')

# Antrieb-Mapping (OpenEV -> Deutsch)
DRIVETRAIN_MAP = {
    'fwd': 'Frontantrieb',
    'rwd': 'Hinterradantrieb',
    'awd': 'Allradantrieb',
    '4wd': 'Allradantrieb',
}

# Marken-Name-Aliase (OpenEV-Name -> DB-Name)
BRAND_ALIASES = {
    'DS Automobiles': 'DS',
    'MINI': 'Mini',
    'Škoda': 'Skoda',
    'MG': 'Mg Roewe',
    'Ssangyong': 'Ssangyong',
    'SsangYong': 'Ssangyong',
    'SEAT': 'Seat',
    'CUPRA': 'Cupra',
    'smart': 'Smart',
    'GWM ORA': 'Ora',
    'Lucid Motors': 'Lucid',
}


class OpenEVImporter:
    """Importiert E-Fahrzeug-Spezifikationen aus OpenEV Data."""

    def __init__(self):
        self._marken_cache = {}    # name_upper -> {id, name}
        self._modelle_cache = {}   # (marke_id, name_upper) -> {id, name, slug}
        self._load_caches()

    def _load_caches(self):
        """Laedt bestehende Marken und Modelle in den Cache."""
        try:
            marken = db.execute("SELECT id, name FROM marken")
            for m in marken:
                self._marken_cache[m['name'].upper()] = m

            modelle = db.execute("SELECT id, marke_id, name, slug FROM modelle")
            for m in modelle:
                self._modelle_cache[(m['marke_id'], m['name'].upper())] = m
            logger.info(
                f"Cache: {len(self._marken_cache)} Marken, "
                f"{len(self._modelle_cache)} Modelle"
            )
        except Exception as e:
            logger.warning(f"Cache konnte nicht geladen werden: {e}")

    # ====================================================================
    #  OEFFENTLICHE METHODEN
    # ====================================================================

    def download_and_import(self):
        """Laedt die neueste OpenEV-CSV herunter und importiert sie."""
        csv_path = self._download_latest()
        if not csv_path:
            logger.error("OpenEV-Daten konnten nicht heruntergeladen werden")
            return None

        return self.import_from_csv(csv_path)

    def import_from_csv(self, csv_path=None):
        """
        Importiert E-Fahrzeug-Daten aus einer OpenEV CSV-Datei.

        Args:
            csv_path: Pfad zur CSV-Datei (Standard: data/openev/openev-data.csv)

        Returns:
            dict: Statistik mit 'imported', 'skipped_brand', 'skipped_model', 'errors'
        """
        if csv_path is None:
            csv_path = OPENEV_CSV_PATH

        if not os.path.exists(csv_path):
            logger.error(f"CSV-Datei nicht gefunden: {csv_path}")
            return None

        logger.info("=" * 60)
        logger.info("OPENEV DATA IMPORT")
        logger.info("=" * 60)
        logger.info(f"Datei: {csv_path}")

        stats = {
            'imported': 0,
            'updated': 0,
            'skipped_brand': 0,
            'skipped_model': 0,
            'errors': 0,
        }

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        logger.info(f"{len(rows)} Eintraege in CSV gefunden")

        # Nach Marke+Modell gruppieren fuer bessere Uebersicht
        current_brand = None
        for row in rows:
            make_name = row.get('make_name', '')
            model_name = row.get('model_name', '')

            if make_name != current_brand:
                current_brand = make_name
                logger.info(f"--- {make_name} ---")

            try:
                result = self._import_single(row, stats)
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  Fehler bei {make_name} {model_name}: {e}")

        logger.info("=" * 60)
        logger.info(
            f"ERGEBNIS: {stats['imported']} importiert, "
            f"{stats['updated']} aktualisiert, "
            f"{stats['skipped_brand']} Marke nicht gefunden, "
            f"{stats['skipped_model']} Modell nicht gefunden, "
            f"{stats['errors']} Fehler"
        )
        logger.info("=" * 60)
        return stats

    # ====================================================================
    #  IMPORT-LOGIK
    # ====================================================================

    def _import_single(self, row, stats):
        """Importiert einen einzelnen OpenEV-Datensatz."""
        make_name = row.get('make_name', '').strip()
        model_name = row.get('model_name', '').strip()
        trim_name = row.get('trim_name', '').strip()
        variant_name = row.get('variant_name', '').strip()
        year = row.get('year', '')

        # 1. Marke in DB finden
        marke = self._find_marke(make_name)
        if not marke:
            stats['skipped_brand'] += 1
            return False

        # 2. Modell in DB finden
        modell = self._find_modell(marke['id'], model_name)
        if not modell:
            stats['skipped_model'] += 1
            return False

        # 3. Varianten-Name zusammenbauen
        varianten_name = self._build_variant_name(
            trim_name, variant_name, year
        )

        # 4. Daten extrahieren und konvertieren
        data = self._extract_ev_data(row)

        # 5. In DB speichern (INSERT oder UPDATE)
        is_update = self._save_variante(modell['id'], varianten_name, data)

        if is_update:
            stats['updated'] += 1
        else:
            stats['imported'] += 1

        logger.debug(
            f"  {make_name} {model_name} '{varianten_name}' -> "
            f"{'aktualisiert' if is_update else 'importiert'}"
        )
        return True

    def _extract_ev_data(self, row):
        """Extrahiert und konvertiert EV-Spezifikationen aus einer CSV-Zeile."""
        data = {
            'kraftstoff': 'Elektro',
        }

        # Leistung
        kw = self._safe_float(row.get('system_power_kw'))
        if kw:
            data['leistung_kw'] = int(kw)
            data['leistung_ps'] = int(kw * 1.36)  # kW -> PS

        # Batterie
        net_kwh = self._safe_float(row.get('battery_capacity_net_kwh'))
        gross_kwh = self._safe_float(row.get('battery_capacity_gross_kwh'))
        data['batteriekapazitaet_kwh'] = net_kwh or gross_kwh

        # Reichweite (WLTP bevorzugt, EPA als Fallback)
        wltp = self._safe_int(row.get('range_wltp_km'))
        epa = self._safe_int(row.get('range_epa_km'))
        data['reichweite_km'] = wltp or epa

        # Ladeleistung
        data['dc_ladeleistung_kw'] = self._safe_float(row.get('dc_max_power_kw'))
        data['ac_ladeleistung_kw'] = self._safe_float(row.get('ac_max_power_kw'))

        # Performance
        data['beschleunigung_0_100'] = self._safe_float(
            row.get('acceleration_0_100_s')
        )
        data['hoechstgeschwindigkeit_kmh'] = self._safe_int(
            row.get('top_speed_kmh')
        )

        # Antrieb
        drivetrain = (row.get('drivetrain') or '').strip().lower()
        data['antrieb'] = DRIVETRAIN_MAP.get(drivetrain)

        # Baujahr
        year = self._safe_int(row.get('year'))
        if year:
            data['baujahr_von'] = year
            data['baujahr_bis'] = year

        # Nur nicht-leere Werte behalten
        return {k: v for k, v in data.items() if v is not None}

    def _save_variante(self, modell_id, name, data):
        """
        Speichert eine E-Fahrzeug-Variante in der Datenbank.
        Aktualisiert bestehende Eintraege (ON DUPLICATE KEY UPDATE).

        Returns:
            bool: True wenn Update, False wenn Insert
        """
        # Pruefen ob Variante schon existiert
        existing = db.execute(
            "SELECT id FROM modell_varianten WHERE modell_id = %s AND name = %s",
            (modell_id, name)
        )
        is_update = len(existing) > 0

        columns = ['modell_id', 'name'] + list(data.keys())
        values = [modell_id, name] + list(data.values())
        placeholders = ', '.join(['%s'] * len(values))
        column_names = ', '.join(columns)

        # UPDATE-Klausel: nur data-Felder (nicht modell_id und name)
        update_parts = ', '.join(
            f"{k} = VALUES({k})" for k in data.keys()
        )

        query = f"""
            INSERT INTO modell_varianten ({column_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_parts}
        """

        # Da es keinen UNIQUE constraint auf (modell_id, name) gibt,
        # muessen wir manuell pruefen
        if is_update:
            set_parts = ', '.join(f"{k} = %s" for k in data.keys())
            db.execute(
                f"UPDATE modell_varianten SET {set_parts} "
                f"WHERE modell_id = %s AND name = %s",
                tuple(list(data.values()) + [modell_id, name])
            )
        else:
            db.execute(
                f"INSERT INTO modell_varianten ({column_names}) "
                f"VALUES ({placeholders})",
                tuple(values)
            )

        return is_update

    # ====================================================================
    #  MATCHING-LOGIK
    # ====================================================================

    def _find_marke(self, openev_name):
        """
        Findet eine Marke in unserer DB basierend auf dem OpenEV-Namen.

        Strategien:
        1. Exakte Uebereinstimmung (case-insensitive)
        2. Alias-Mapping
        3. Teilstring-Matching
        """
        if not openev_name:
            return None

        upper = openev_name.upper()

        # 1. Exakte Uebereinstimmung
        if upper in self._marken_cache:
            return self._marken_cache[upper]

        # 2. Alias-Mapping
        alias = BRAND_ALIASES.get(openev_name)
        if alias and alias.upper() in self._marken_cache:
            return self._marken_cache[alias.upper()]

        # 3. Teilstring-Matching (OpenEV-Name in DB-Name oder umgekehrt)
        for db_name_upper, marke in self._marken_cache.items():
            if upper in db_name_upper or db_name_upper in upper:
                return marke

        return None

    def _find_modell(self, marke_id, openev_model_name):
        """
        Findet ein Modell in unserer DB basierend auf dem OpenEV-Modellnamen.

        Strategien:
        1. Exakte Uebereinstimmung (case-insensitive)
        2. Normalisierter Vergleich (Punkte, Bindestriche, Leerzeichen)
        3. Teilstring-Matching (fuer KBA-Kombinationen wie "A3, S3, RS3")
        """
        if not openev_model_name:
            return None

        upper = openev_model_name.upper()
        normalized = self._normalize_model_name(openev_model_name)

        # 1. Exakte Uebereinstimmung
        if (marke_id, upper) in self._modelle_cache:
            return self._modelle_cache[(marke_id, upper)]

        # 2. Normalisierter Vergleich
        for (mid, db_name_upper), modell in self._modelle_cache.items():
            if mid != marke_id:
                continue

            db_normalized = self._normalize_model_name(db_name_upper)

            if normalized == db_normalized:
                return modell

        # 3. Teilstring-Matching (fuer KBA-Kombinamen)
        for (mid, db_name_upper), modell in self._modelle_cache.items():
            if mid != marke_id:
                continue

            # KBA hat oft "A3, S3, RS3" - pruefen ob OpenEV-Name drin ist
            db_parts = [p.strip() for p in db_name_upper.split(',')]
            for part in db_parts:
                if self._normalize_model_name(part) == normalized:
                    return modell

            # Pruefen ob OpenEV-Name als Teilstring vorkommt
            if normalized in db_normalized or db_normalized in normalized:
                return modell

        # 4. Levenshtein-artiger Vergleich: nur erste N Zeichen
        for (mid, db_name_upper), modell in self._modelle_cache.items():
            if mid != marke_id:
                continue

            db_norm = self._normalize_model_name(db_name_upper)
            # Wenn einer ein Praefix des anderen ist (z.B. "IONIQ5" vs "IONIQ 5")
            if len(normalized) >= 3 and len(db_norm) >= 3:
                if normalized.startswith(db_norm[:4]) or \
                   db_norm.startswith(normalized[:4]):
                    # Aehnlichkeit pruefen
                    if self._similarity(normalized, db_norm) > 0.7:
                        return modell

        return None

    def _normalize_model_name(self, name):
        """
        Normalisiert einen Modellnamen fuer den Vergleich.
        Entfernt Punkte, Bindestriche, Leerzeichen und macht alles uppercase.
        """
        if not name:
            return ''
        # Uppercase
        n = name.upper()
        # Punkte, Bindestriche und Leerzeichen entfernen
        n = re.sub(r'[\s.\-_/]+', '', n)
        return n

    def _similarity(self, a, b):
        """Einfache Aehnlichkeitsmetrik (Jaccard auf Zeichen-Bigrammen)."""
        if not a or not b:
            return 0.0

        def bigrams(s):
            return set(s[i:i+2] for i in range(len(s)-1))

        ba, bb = bigrams(a), bigrams(b)
        if not ba or not bb:
            return 0.0

        intersection = len(ba & bb)
        union = len(ba | bb)
        return intersection / union if union > 0 else 0.0

    # ====================================================================
    #  DOWNLOAD
    # ====================================================================

    def _download_latest(self):
        """Laedt die neueste OpenEV CSV-Datei von GitHub herunter."""
        logger.info("Lade neueste OpenEV-Daten von GitHub herunter...")

        try:
            # Release-Info abrufen
            resp = requests.get(
                OPENEV_RELEASES_API,
                headers={'Accept': 'application/vnd.github.v3+json'},
                timeout=15,
            )
            resp.raise_for_status()
            release = resp.json()

            # CSV-Asset finden
            csv_url = None
            for asset in release.get('assets', []):
                if asset['name'].endswith('.csv'):
                    csv_url = asset['browser_download_url']
                    break

            if not csv_url:
                logger.error("Keine CSV-Datei im Release gefunden")
                return None

            # Herunterladen
            csv_dir = os.path.dirname(OPENEV_CSV_PATH)
            os.makedirs(csv_dir, exist_ok=True)

            logger.info(f"Download: {csv_url}")
            resp = requests.get(csv_url, timeout=60)
            resp.raise_for_status()

            with open(OPENEV_CSV_PATH, 'wb') as f:
                f.write(resp.content)

            file_size = os.path.getsize(OPENEV_CSV_PATH)
            logger.info(
                f"Gespeichert: {OPENEV_CSV_PATH} ({file_size / 1024:.1f} KB)"
            )
            return OPENEV_CSV_PATH

        except Exception as e:
            logger.error(f"Download fehlgeschlagen: {e}")
            # Fallback: Existierende Datei verwenden
            if os.path.exists(OPENEV_CSV_PATH):
                logger.info("Verwende existierende CSV-Datei als Fallback")
                return OPENEV_CSV_PATH
            return None

    # ====================================================================
    #  HILFSFUNKTIONEN
    # ====================================================================

    @staticmethod
    def _build_variant_name(trim_name, variant_name, year):
        """Baut einen lesbaren Varianten-Namen zusammen."""
        parts = []
        if trim_name:
            parts.append(trim_name)
        if variant_name and variant_name != trim_name:
            parts.append(variant_name)
        name = ' '.join(parts) if parts else 'Standard'
        if year:
            name = f"{name} ({year})"
        return name

    @staticmethod
    def _safe_float(value):
        """Konvertiert einen Wert sicher zu float, None bei Fehler."""
        if not value or value == '':
            return None
        try:
            result = float(value)
            return result if result > 0 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value):
        """Konvertiert einen Wert sicher zu int, None bei Fehler."""
        if not value or value == '':
            return None
        try:
            result = int(float(value))
            return result if result > 0 else None
        except (ValueError, TypeError):
            return None
