"""
DB-Writer: Schreibt geparste und normalisierte Daten in MariaDB.
Verwaltet Marken, Modelle und Neuzulassungszahlen.
"""
import logging
from datetime import datetime

from src.database import db
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class DBWriter:
    """Schreibt normalisierte KBA-Daten in die MariaDB-Datenbank."""

    def __init__(self):
        self._marken_cache = {}   # name -> id
        self._modell_cache = {}   # (marke_id, name) -> id
        self._load_caches()

    def _load_caches(self):
        """Laedt bestehende Marken und Modelle in den Cache."""
        try:
            marken = db.execute("SELECT id, name FROM marken")
            for m in marken:
                self._marken_cache[m['name'].upper()] = m['id']
            logger.info(f"Cache geladen: {len(self._marken_cache)} Marken")

            modelle = db.execute("SELECT id, marke_id, name FROM modelle")
            for m in modelle:
                self._modell_cache[(m['marke_id'], m['name'].upper())] = m['id']
            logger.info(f"Cache geladen: {len(self._modell_cache)} Modelle")
        except Exception as e:
            logger.warning(f"Cache konnte nicht geladen werden: {e}")

    def get_or_create_marke(self, name):
        """
        Gibt die ID einer Marke zurueck. Erstellt sie falls noetig.

        Args:
            name: Markenname (wird normalisiert)

        Returns:
            int: Marken-ID
        """
        normalized = DataNormalizer.normalize_marke(name)
        if not normalized:
            return None

        cache_key = normalized.upper()
        if cache_key in self._marken_cache:
            return self._marken_cache[cache_key]

        slug = DataNormalizer.generate_slug(normalized)

        marke_id = db.insert_or_update(
            """INSERT INTO marken (name, slug)
               VALUES (%s, %s)
               ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
            (normalized, slug)
        )

        # Wenn LAST_INSERT_ID 0 ist (kein Update), nochmal lesen
        if not marke_id:
            result = db.execute("SELECT id FROM marken WHERE name = %s", (normalized,))
            if result:
                marke_id = result[0]['id']

        self._marken_cache[cache_key] = marke_id
        logger.debug(f"Marke: {normalized} -> ID {marke_id}")
        return marke_id

    def get_or_create_modell(self, marke_id, name, segment=None):
        """
        Gibt die ID eines Modells zurueck. Erstellt es falls noetig.

        Args:
            marke_id: ID der Marke
            name: Modellname (wird normalisiert)
            segment: Fahrzeugsegment (optional)

        Returns:
            int: Modell-ID oder None
        """
        normalized = DataNormalizer.normalize_modell(name)
        if not normalized or not marke_id:
            return None

        cache_key = (marke_id, normalized.upper())
        if cache_key in self._modell_cache:
            return self._modell_cache[cache_key]

        # Slug aus Markenname + Modellname
        marke_result = db.execute("SELECT name FROM marken WHERE id = %s", (marke_id,))
        marke_name = marke_result[0]['name'] if marke_result else ''
        slug = DataNormalizer.generate_slug(f"{marke_name} {normalized}")

        modell_id = db.insert_or_update(
            """INSERT INTO modelle (marke_id, name, slug, segment)
               VALUES (%s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   id=LAST_INSERT_ID(id),
                   segment=COALESCE(VALUES(segment), segment)""",
            (marke_id, normalized, slug, segment)
        )

        if not modell_id:
            result = db.execute(
                "SELECT id FROM modelle WHERE marke_id = %s AND name = %s",
                (marke_id, normalized)
            )
            if result:
                modell_id = result[0]['id']

        self._modell_cache[cache_key] = modell_id
        logger.debug(f"Modell: {normalized} (Marke {marke_id}) -> ID {modell_id}")
        return modell_id

    def write_neuzulassungen(self, data_rows, land_code='DE', quelle_kuerzel='FZ10'):
        """
        Schreibt Neuzulassungsdaten in die Datenbank.

        Args:
            data_rows: Liste von Dicts mit keys:
                - marke: Markenname
                - modell: Modellname
                - jahr: Jahr
                - monat: Monat
                - anzahl: Anzahl Neuzulassungen
                - kraftstoff: Kraftstoffart (optional)
                - segment: Segment (optional)
            land_code: ISO-Laendercode
            quelle_kuerzel: Datenquellen-Kuerzel

        Returns:
            int: Anzahl der geschriebenen Datensaetze
        """
        land_id = db.get_land_id(land_code)
        quelle_id = db.get_quelle_id(quelle_kuerzel, land_code)

        if not land_id or not quelle_id:
            logger.error(f"Land '{land_code}' oder Quelle '{quelle_kuerzel}' nicht gefunden!")
            return 0

        written = 0
        skipped = 0

        for row in data_rows:
            try:
                marke_id = self.get_or_create_marke(row.get('marke'))
                if not marke_id:
                    skipped += 1
                    continue

                modell_id = self.get_or_create_modell(
                    marke_id,
                    row.get('modell'),
                    row.get('segment')
                )
                if not modell_id:
                    skipped += 1
                    continue

                anzahl = DataNormalizer.normalize_anzahl(row.get('anzahl', 0))
                if anzahl <= 0:
                    skipped += 1
                    continue

                kraftstoff = DataNormalizer.normalize_kraftstoff(
                    row.get('kraftstoff')
                ) if row.get('kraftstoff') else None

                db.insert_or_update(
                    """INSERT INTO neuzulassungen
                       (land_id, modell_id, jahr, monat, anzahl, kraftstoff, quelle_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE anzahl = VALUES(anzahl)""",
                    (land_id, modell_id, row['jahr'], row['monat'],
                     anzahl, kraftstoff, quelle_id)
                )
                written += 1

            except Exception as e:
                logger.error(f"Fehler beim Schreiben: {row} -> {e}")
                skipped += 1

        logger.info(
            f"Neuzulassungen geschrieben: {written} Datensaetze, "
            f"{skipped} uebersprungen ({quelle_kuerzel})"
        )
        return written

    def write_aggregiert(self, data_rows, land_code='DE', quelle_kuerzel='FZ8'):
        """
        Schreibt aggregierte Neuzulassungsdaten (z.B. aus FZ8).

        Args:
            data_rows: Liste von Dicts mit keys:
                - jahr, monat, fahrzeugart, region, kraftstoff, anzahl
        """
        land_id = db.get_land_id(land_code)
        quelle_id = db.get_quelle_id(quelle_kuerzel, land_code)

        if not land_id or not quelle_id:
            return 0

        written = 0
        for row in data_rows:
            try:
                anzahl = DataNormalizer.normalize_anzahl(row.get('anzahl', 0))
                if anzahl <= 0:
                    continue

                db.insert_or_update(
                    """INSERT INTO neuzulassungen_aggregiert
                       (land_id, jahr, monat, fahrzeugart, region, kraftstoff, anzahl, quelle_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (land_id, row['jahr'], row['monat'],
                     row.get('fahrzeugart'), row.get('region'),
                     DataNormalizer.normalize_kraftstoff(row.get('kraftstoff')),
                     anzahl, quelle_id)
                )
                written += 1
            except Exception as e:
                logger.error(f"Fehler beim Schreiben (aggregiert): {row} -> {e}")

        logger.info(f"Aggregierte Daten geschrieben: {written} ({quelle_kuerzel})")
        return written

    def log_import(self, quelle_kuerzel, dateiname, jahr, monat, status,
                   rows_imported=0, error_message=None):
        """Protokolliert einen Import-Vorgang."""
        quelle_id = db.get_quelle_id(quelle_kuerzel)
        if not quelle_id:
            return

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        db.insert_or_update(
            """INSERT INTO import_log
               (quelle_id, dateiname, jahr, monat, status, rows_imported,
                error_message, started_at, finished_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   status = VALUES(status),
                   rows_imported = VALUES(rows_imported),
                   error_message = VALUES(error_message),
                   finished_at = VALUES(finished_at)""",
            (quelle_id, dateiname, jahr, monat, status,
             rows_imported, error_message,
             now if status == 'running' else None,
             now if status in ('success', 'error') else None)
        )

    def is_already_imported(self, quelle_kuerzel, dateiname):
        """Prueft ob eine Datei bereits erfolgreich importiert wurde."""
        quelle_id = db.get_quelle_id(quelle_kuerzel)
        if not quelle_id:
            return False

        result = db.execute(
            """SELECT id FROM import_log
               WHERE quelle_id = %s AND dateiname = %s AND status = 'success'""",
            (quelle_id, dateiname)
        )
        return len(result) > 0
