"""
Scheduler: Prueft taeglich auf neue KBA-Daten und startet den Import.
Unterstuetzt alle monatlichen (FZ10, FZ11, FZ8, FZ9) und
jaehrlichen (FZ4, FZ14, FZ28, FZ1) KBA-Publikationen.
"""
import logging
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import Config
from src.downloader import KBADownloader
from src.parsers.fz10_parser import FZ10Parser
from src.parsers.fz11_parser import FZ11Parser
from src.parsers.fz8_parser import FZ8Parser
from src.parsers.fz9_parser import FZ9Parser
from src.parsers.fz4_parser import FZ4Parser
from src.parsers.fz14_parser import FZ14Parser
from src.parsers.fz28_parser import FZ28Parser
from src.parsers.fz1_parser import FZ1Parser
from src.db_writer import DBWriter
from src.database import db
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


# Maps publication type to (parser class, writer method name).
# 'neuzulassungen' -> write_neuzulassungen, 'aggregiert' -> write_aggregiert,
# 'bestand' -> custom _write_bestand handler.
MONTHLY_PARSERS = {
    'FZ10': (FZ10Parser, 'neuzulassungen'),
    'FZ11': (FZ11Parser, 'neuzulassungen'),
    'FZ8':  (FZ8Parser,  'aggregiert'),
    'FZ9':  (FZ9Parser,  'aggregiert'),
}

YEARLY_PARSERS = {
    'FZ4':  (FZ4Parser,  'neuzulassungen'),
    'FZ14': (FZ14Parser, 'aggregiert'),
    'FZ28': (FZ28Parser, 'neuzulassungen'),
    'FZ1':  (FZ1Parser,  'bestand'),
}


def _write_bestand(db_writer, data):
    """Schreibt Fahrzeugbestand-Daten (FZ1) in die DB."""
    land_id = db.get_land_id('DE')
    quelle_id = db.get_quelle_id('FZ1')
    written = 0

    for row in data:
        try:
            marke_id = db_writer.get_or_create_marke(row.get('marke')) if row.get('marke') else None
            anzahl = DataNormalizer.normalize_anzahl(row.get('anzahl', 0))
            if anzahl <= 0:
                continue

            db.insert_or_update(
                """INSERT INTO fahrzeugbestand
                   (land_id, marke_id, jahr, anzahl, kraftstoff, quelle_id)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE anzahl = VALUES(anzahl)""",
                (land_id, marke_id, row['jahr'], anzahl,
                 DataNormalizer.normalize_kraftstoff(row.get('kraftstoff')),
                 quelle_id)
            )
            written += 1
        except Exception as e:
            logger.error(f"Bestand-Schreibfehler: {e}")

    return written


class DataScheduler:
    """Taeglich nach neuen KBA-Daten pruefen und importieren."""

    def __init__(self):
        self.downloader = KBADownloader()
        self.db_writer = DBWriter()
        self.scheduler = BlockingScheduler()

    def _import_file(self, pub_type, parser, writer_type, filepath):
        """Importiert eine einzelne Datei mit dem passenden Parser und Writer."""
        filename = filepath.name

        if self.db_writer.is_already_imported(pub_type, filename):
            logger.debug(f"Bereits importiert: {filename}")
            return 0

        self.db_writer.log_import(pub_type, filename, None, None, 'running')

        try:
            data = parser.parse(filepath)
            if not data:
                self.db_writer.log_import(pub_type, filename, None, None, 'error',
                                          error_message='Keine Daten geparst')
                return 0

            if writer_type == 'neuzulassungen':
                rows = self.db_writer.write_neuzulassungen(data, quelle_kuerzel=pub_type)
            elif writer_type == 'aggregiert':
                rows = self.db_writer.write_aggregiert(data, quelle_kuerzel=pub_type)
            elif writer_type == 'bestand':
                rows = _write_bestand(self.db_writer, data)
            else:
                raise ValueError(f"Unbekannter Writer-Typ: {writer_type}")

            self.db_writer.log_import(pub_type, filename, None, None, 'success', rows)
            logger.info(f"Importiert: {pub_type}/{filename} -> {rows} Datensaetze")
            return rows

        except Exception as e:
            logger.error(f"Import-Fehler {pub_type}/{filename}: {e}")
            self.db_writer.log_import(pub_type, filename, None, None, 'error',
                                      error_message=str(e))
            return 0

    def check_and_import(self):
        """Prueft auf neue monatliche und jaehrliche Daten und importiert sie."""
        logger.info("=== Scheduler: Pruefe auf neue Daten ===")

        try:
            # 1. Monatliche Daten (FZ10, FZ11, FZ8, FZ9)
            new_files = self.downloader.check_for_new_data()

            if new_files:
                for pub_type, (parser_cls, writer_type) in MONTHLY_PARSERS.items():
                    if pub_type in new_files:
                        parser = parser_cls()
                        for filepath in new_files[pub_type]:
                            self._import_file(pub_type, parser, writer_type, filepath)

            # 2. Jaehrliche Daten (FZ4, FZ14, FZ28, FZ1)
            #    Pruefen ob neue Jahresdateien verfuegbar sind
            self._check_yearly_data()

            logger.info("=== Scheduler: Import abgeschlossen ===")

        except Exception as e:
            logger.error(f"Scheduler-Fehler: {e}", exc_info=True)

    def _check_yearly_data(self):
        """Prueft ob neue jaehrliche KBA-Dateien verfuegbar sind."""
        now = datetime.now()

        for pub_type, (parser_cls, writer_type) in YEARLY_PARSERS.items():
            # Pruefe aktuelles und vorheriges Jahr
            for year in [now.year, now.year - 1]:
                if self.downloader.is_already_downloaded(pub_type, year):
                    # Datei schon da — trotzdem pruefen ob bereits importiert
                    filepath = self.downloader._get_filepath(pub_type, year)
                    if filepath.exists():
                        parser = parser_cls()
                        self._import_file(pub_type, parser, writer_type, filepath)
                    continue

                filepath = self.downloader.download_file(pub_type, year)
                if filepath:
                    logger.info(f"Neue Jahresdatei: {pub_type} {year}")
                    parser = parser_cls()
                    self._import_file(pub_type, parser, writer_type, filepath)

    def start(self):
        """Startet den Scheduler."""
        self.scheduler.add_job(
            self.check_and_import,
            trigger=CronTrigger(
                hour=Config.SCHEDULER_CHECK_HOUR,
                minute=Config.SCHEDULER_CHECK_MINUTE
            ),
            id='check_new_data',
            name='KBA Daten pruefen (alle Publikationstypen)',
            replace_existing=True
        )

        logger.info(
            f"Scheduler gestartet: Taeglich um "
            f"{Config.SCHEDULER_CHECK_HOUR:02d}:{Config.SCHEDULER_CHECK_MINUTE:02d} "
            f"(Monatlich: {list(MONTHLY_PARSERS.keys())}, "
            f"Jaehrlich: {list(YEARLY_PARSERS.keys())})"
        )
        self.scheduler.start()

    def stop(self):
        """Stoppt den Scheduler."""
        self.scheduler.shutdown()
        logger.info("Scheduler gestoppt.")
