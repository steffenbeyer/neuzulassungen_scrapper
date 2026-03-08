"""
KBA-Downloader: Laedt XLSX-Dateien vom Kraftfahrt-Bundesamt herunter.
Unterstuetzt sowohl monatliche als auch jaehrliche Publikationen.
"""
import os
import time
import logging
from datetime import datetime
from pathlib import Path

import requests

from config import Config

logger = logging.getLogger(__name__)


class KBADownloader:
    """Laedt Excel-Dateien von der KBA-Website herunter."""

    # URL-Muster fuer die verschiedenen Publikationen
    URL_PATTERNS = {
        # Monatliche Publikationen
        'FZ10': '{base}/FZ10/fz10_{year}_{month:02d}.xlsx?__blob=publicationFile&v=2',
        'FZ11': '{base}/FZ11/fz11_{year}_{month:02d}.xlsx?__blob=publicationFile&v=2',
        'FZ8':  '{base}/FZ8/fz8_{year}_{month:02d}.xlsx?__blob=publicationFile&v=2',
        'FZ9':  '{base}/FZ9/fz9_{year}_{month:02d}.xlsx?__blob=publicationFile&v=2',
        'FZ28': '{base}/FZ28/fz28_{year}_{month:02d}.xlsx?__blob=publicationFile&v=2',
        # Jaehrliche Publikationen
        'FZ4':  '{base}/FZ4/fz4_{year}.xlsx?__blob=publicationFile&v=2',
        'FZ14': '{base}/FZ14/fz14_{year}.xlsx?__blob=publicationFile&v=2',
        'FZ1':  '{base}/FZ1/fz1_{year}.xlsx?__blob=publicationFile&v=2',
    }

    MONTHLY_TYPES = ['FZ10', 'FZ11', 'FZ8', 'FZ9', 'FZ28']
    YEARLY_TYPES = ['FZ4', 'FZ14', 'FZ1']

    def __init__(self, download_dir=None):
        self.download_dir = Path(download_dir or Config.DOWNLOAD_DIR)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': Config.KBA_USER_AGENT,
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _build_url(self, pub_type, year, month=None):
        """Erstellt die Download-URL fuer eine bestimmte Publikation."""
        pattern = self.URL_PATTERNS.get(pub_type)
        if not pattern:
            raise ValueError(f"Unbekannter Publikationstyp: {pub_type}")

        if pub_type in self.MONTHLY_TYPES:
            return pattern.format(base=Config.KBA_BASE_URL, year=year, month=month)
        else:
            return pattern.format(base=Config.KBA_BASE_URL, year=year)

    def _get_filename(self, pub_type, year, month=None):
        """Generiert den lokalen Dateinamen fuer eine Publikation."""
        if pub_type in self.MONTHLY_TYPES:
            return f"{pub_type.lower()}_{year}_{month:02d}.xlsx"
        else:
            return f"{pub_type.lower()}_{year}.xlsx"

    def _get_filepath(self, pub_type, year, month=None):
        """Gibt den vollstaendigen Dateipfad zurueck."""
        subdir = self.download_dir / pub_type.lower()
        subdir.mkdir(parents=True, exist_ok=True)
        filename = self._get_filename(pub_type, year, month)
        return subdir / filename

    def is_already_downloaded(self, pub_type, year, month=None):
        """Prueft ob eine Datei bereits heruntergeladen wurde."""
        filepath = self._get_filepath(pub_type, year, month)
        return filepath.exists() and filepath.stat().st_size > 0

    def download_file(self, pub_type, year, month=None, force=False):
        """
        Laedt eine einzelne Datei herunter.

        Returns:
            Path: Pfad zur heruntergeladenen Datei, oder None bei Fehler
        """
        filepath = self._get_filepath(pub_type, year, month)

        if not force and self.is_already_downloaded(pub_type, year, month):
            logger.debug(f"Bereits vorhanden: {filepath.name}")
            return filepath

        url = self._build_url(pub_type, year, month)
        month_str = f"/{month:02d}" if month else ""
        logger.info(f"Lade herunter: {pub_type} {year}{month_str} -> {filepath.name}")

        try:
            response = self.session.get(url, timeout=Config.KBA_REQUEST_TIMEOUT)

            if response.status_code == 200:
                # Pruefen ob wirklich eine Excel-Datei zurueckkommt
                content_type = response.headers.get('Content-Type', '')
                if 'html' in content_type.lower() and len(response.content) < 5000:
                    logger.warning(f"Keine Excel-Datei fuer {pub_type} {year}{month_str} (HTML-Antwort)")
                    return None

                filepath.write_bytes(response.content)
                logger.info(f"Erfolgreich: {filepath.name} ({len(response.content)} Bytes)")
                return filepath

            elif response.status_code == 404:
                logger.debug(f"Nicht verfuegbar: {pub_type} {year}{month_str}")
                return None
            else:
                logger.warning(f"HTTP {response.status_code} fuer {pub_type} {year}{month_str}")
                return None

        except requests.RequestException as e:
            logger.error(f"Download-Fehler fuer {pub_type} {year}{month_str}: {e}")
            return None

    def download_monthly_range(self, pub_type, start_year=None, start_month=1,
                                end_year=None, end_month=None):
        """
        Laedt alle monatlichen Dateien eines Typs in einem Zeitraum herunter.

        Returns:
            list: Liste der erfolgreich heruntergeladenen Dateipfade
        """
        if pub_type not in self.MONTHLY_TYPES:
            raise ValueError(f"{pub_type} ist keine monatliche Publikation")

        start_year = start_year or Config.KBA_START_YEAR
        now = datetime.now()
        end_year = end_year or now.year
        end_month = end_month or now.month

        downloaded = []
        skipped = 0
        failed = 0

        for year in range(start_year, end_year + 1):
            m_start = start_month if year == start_year else 1
            m_end = end_month if year == end_year else 12

            for month in range(m_start, m_end + 1):
                if self.is_already_downloaded(pub_type, year, month):
                    skipped += 1
                    continue

                filepath = self.download_file(pub_type, year, month)
                if filepath:
                    downloaded.append(filepath)
                else:
                    failed += 1

                # Rate Limiting
                time.sleep(Config.KBA_REQUEST_DELAY)

        logger.info(
            f"{pub_type} Download abgeschlossen: "
            f"{len(downloaded)} neu, {skipped} uebersprungen, {failed} fehlgeschlagen"
        )
        return downloaded

    def download_yearly_range(self, pub_type, start_year=None, end_year=None):
        """
        Laedt alle jaehrlichen Dateien eines Typs herunter.

        Returns:
            list: Liste der erfolgreich heruntergeladenen Dateipfade
        """
        if pub_type not in self.YEARLY_TYPES:
            raise ValueError(f"{pub_type} ist keine jaehrliche Publikation")

        start_year = start_year or Config.KBA_START_YEAR
        end_year = end_year or datetime.now().year

        downloaded = []

        for year in range(start_year, end_year + 1):
            if self.is_already_downloaded(pub_type, year):
                continue

            filepath = self.download_file(pub_type, year)
            if filepath:
                downloaded.append(filepath)

            time.sleep(Config.KBA_REQUEST_DELAY)

        logger.info(f"{pub_type} Download abgeschlossen: {len(downloaded)} neue Dateien")
        return downloaded

    def download_all_monthly(self, types=None):
        """Laedt alle monatlichen Publikationen herunter."""
        types = types or self.MONTHLY_TYPES
        all_downloaded = {}
        for pub_type in types:
            logger.info(f"=== Starte Download: {pub_type} ===")
            all_downloaded[pub_type] = self.download_monthly_range(pub_type)
        return all_downloaded

    def download_all_yearly(self, types=None):
        """Laedt alle jaehrlichen Publikationen herunter."""
        types = types or self.YEARLY_TYPES
        all_downloaded = {}
        for pub_type in types:
            logger.info(f"=== Starte Download: {pub_type} ===")
            all_downloaded[pub_type] = self.download_yearly_range(pub_type)
        return all_downloaded

    def check_for_new_data(self):
        """
        Prueft ob neue monatliche Daten verfuegbar sind.
        Wird vom Scheduler taeglich aufgerufen.

        Returns:
            dict: Neu heruntergeladene Dateien pro Typ
        """
        now = datetime.now()
        new_files = {}

        for pub_type in self.MONTHLY_TYPES:
            # Pruefe aktuellen und vorherigen Monat
            for month_offset in [0, -1]:
                year = now.year
                month = now.month + month_offset
                if month <= 0:
                    month += 12
                    year -= 1

                if not self.is_already_downloaded(pub_type, year, month):
                    filepath = self.download_file(pub_type, year, month)
                    if filepath:
                        if pub_type not in new_files:
                            new_files[pub_type] = []
                        new_files[pub_type].append(filepath)
                    time.sleep(Config.KBA_REQUEST_DELAY)

        if new_files:
            total = sum(len(v) for v in new_files.values())
            logger.info(f"Neue Daten gefunden: {total} Dateien")
        else:
            logger.info("Keine neuen Daten verfuegbar.")

        return new_files
