"""
Scheduler: Prueft taeglich auf neue KBA-Daten und startet den Import.
"""
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import Config
from src.downloader import KBADownloader
from src.parsers.fz10_parser import FZ10Parser
from src.db_writer import DBWriter
from src.database import db

logger = logging.getLogger(__name__)


class DataScheduler:
    """Taeglich nach neuen KBA-Daten pruefen und importieren."""

    def __init__(self):
        self.downloader = KBADownloader()
        self.db_writer = DBWriter()
        self.scheduler = BlockingScheduler()

    def check_and_import(self):
        """Prueft auf neue Daten und importiert sie."""
        logger.info("=== Scheduler: Pruefe auf neue Daten ===")

        try:
            new_files = self.downloader.check_for_new_data()

            if not new_files:
                logger.info("Keine neuen Daten gefunden.")
                return

            # FZ10 importieren
            if 'FZ10' in new_files:
                parser = FZ10Parser()
                for filepath in new_files['FZ10']:
                    try:
                        filename = filepath.name
                        if self.db_writer.is_already_imported('FZ10', filename):
                            logger.info(f"Bereits importiert: {filename}")
                            continue

                        self.db_writer.log_import('FZ10', filename,
                                                   *parser.extract_year_month_from_filename(),
                                                   'running')

                        data = parser.parse(filepath)
                        rows = self.db_writer.write_neuzulassungen(data, quelle_kuerzel='FZ10')

                        self.db_writer.log_import('FZ10', filename,
                                                   *parser.extract_year_month_from_filename(),
                                                   'success', rows)
                    except Exception as e:
                        logger.error(f"Import-Fehler {filepath}: {e}")
                        self.db_writer.log_import('FZ10', filepath.name,
                                                   None, None, 'error',
                                                   error_message=str(e))

            # TODO: Weitere Parser (FZ8, FZ11, etc.) hier einhaengen

            logger.info("=== Scheduler: Import abgeschlossen ===")

        except Exception as e:
            logger.error(f"Scheduler-Fehler: {e}")

    def start(self):
        """Startet den Scheduler."""
        self.scheduler.add_job(
            self.check_and_import,
            trigger=CronTrigger(
                hour=Config.SCHEDULER_CHECK_HOUR,
                minute=Config.SCHEDULER_CHECK_MINUTE
            ),
            id='check_new_data',
            name='KBA Daten pruefen',
            replace_existing=True
        )

        logger.info(
            f"Scheduler gestartet: Taeglich um "
            f"{Config.SCHEDULER_CHECK_HOUR:02d}:{Config.SCHEDULER_CHECK_MINUTE:02d}"
        )
        self.scheduler.start()

    def stop(self):
        """Stoppt den Scheduler."""
        self.scheduler.shutdown()
        logger.info("Scheduler gestoppt.")
