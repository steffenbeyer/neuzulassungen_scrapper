"""
ACEA Importer: Laedt europaeische Zulassungsdaten von der EZB herunter und importiert sie.
"""
import logging
import requests
from pathlib import Path

from config import Config
from src.database import db
from src.parsers.acea_parser import ACEAParser, COUNTRY_NAMES
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)

# EZB-Daten-URL (Bulk Download)
ECB_DATA_URL = 'https://data.ecb.europa.eu/data/datasets/CAR'
ECB_BULK_DOWNLOAD = 'https://data-api.ecb.europa.eu/service/data/CAR/?format=csvdata'


class ACEAImporter:
    """Importiert ACEA/EZB Daten in die Datenbank."""

    def __init__(self):
        self.parser = ACEAParser()
        self.download_dir = Path(Config.DOWNLOAD_DIR) / 'acea'
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def download_ecb_data(self):
        """
        Laedt die EZB CAR-Daten herunter.

        Returns:
            Path: Pfad zur heruntergeladenen Datei oder None
        """
        filepath = self.download_dir / 'ecb_car_data.csv'

        logger.info("Lade EZB CAR-Daten herunter...")

        try:
            response = requests.get(
                ECB_BULK_DOWNLOAD,
                headers={'User-Agent': Config.KBA_USER_AGENT},
                timeout=120
            )

            if response.status_code == 200:
                filepath.write_text(response.text, encoding='utf-8')
                logger.info(f"EZB-Daten heruntergeladen: {filepath} ({len(response.content)} Bytes)")
                return filepath
            else:
                logger.error(f"EZB-Download fehlgeschlagen: HTTP {response.status_code}")
                return None

        except requests.RequestException as e:
            logger.error(f"EZB-Download Fehler: {e}")
            return None

    def ensure_countries_exist(self):
        """Stellt sicher, dass alle EU-Laender in der DB existieren."""
        for code, name in COUNTRY_NAMES.items():
            db.insert_or_update(
                "INSERT IGNORE INTO laender (code, name) VALUES (%s, %s)",
                (code, name)
            )
        logger.info(f"{len(COUNTRY_NAMES)} Laender sichergestellt")

    def ensure_acea_quelle(self, land_code):
        """Stellt sicher, dass die ACEA-Datenquelle fuer ein Land existiert."""
        land_id = db.get_land_id(land_code)
        if not land_id:
            return None

        db.insert_or_update(
            """INSERT IGNORE INTO datenquellen (land_id, kuerzel, name, typ, url_pattern)
               VALUES (%s, 'ACEA_CAR', 'ACEA/EZB PKW-Neuzulassungen', 'monatlich', %s)""",
            (land_id, ECB_BULK_DOWNLOAD)
        )

        return db.get_quelle_id('ACEA_CAR', land_code)

    def import_data(self, filepath=None):
        """
        Importiert ACEA-Daten in die Datenbank.

        Args:
            filepath: Pfad zur CSV-Datei (optional, wird sonst heruntergeladen)

        Returns:
            dict: Ergebnis-Zusammenfassung pro Land
        """
        if not filepath:
            filepath = self.download_ecb_data()
            if not filepath:
                return {}

        # Laender sicherstellen
        self.ensure_countries_exist()

        # Daten parsen
        logger.info("Parse ACEA-Daten...")
        data = self.parser.parse_ecb_format(filepath)

        if not data:
            logger.warning("Keine ACEA-Daten geparst!")
            return {}

        # Nach Land gruppieren und importieren
        by_country = {}
        for row in data:
            code = row['land_code']
            if code not in by_country:
                by_country[code] = []
            by_country[code].append(row)

        results = {}
        for land_code, rows in by_country.items():
            quelle_id = self.ensure_acea_quelle(land_code)
            land_id = db.get_land_id(land_code)

            if not quelle_id or not land_id:
                logger.warning(f"Quelle/Land nicht gefunden fuer {land_code}")
                continue

            written = 0
            for row in rows:
                try:
                    # ACEA-Daten sind aggregiert (keine Marken/Modelle),
                    # daher in neuzulassungen_aggregiert speichern
                    db.insert_or_update(
                        """INSERT INTO neuzulassungen_aggregiert
                           (land_id, jahr, monat, fahrzeugart, region, kraftstoff, anzahl, quelle_id)
                           VALUES (%s, %s, %s, 'PKW', NULL, NULL, %s, %s)
                           ON DUPLICATE KEY UPDATE anzahl = VALUES(anzahl)""",
                        (land_id, row['jahr'], row['monat'], row['anzahl'], quelle_id)
                    )
                    written += 1
                except Exception as e:
                    logger.error(f"Fehler beim Import ({land_code}): {e}")

            results[land_code] = written
            logger.info(f"ACEA {land_code}: {written} Datensaetze importiert")

        total = sum(results.values())
        logger.info(f"ACEA Import abgeschlossen: {total} Datensaetze aus {len(results)} Laendern")
        return results
