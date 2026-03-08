"""
Konfiguration fuer den AlleZulassungen Scrapper.
Liest Einstellungen aus Umgebungsvariablen oder .env Datei.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Datenbank
    DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
    DB_PORT = int(os.getenv('DB_PORT', '3306'))
    DB_NAME = os.getenv('DB_NAME', 'allezulassungen')
    DB_USER = os.getenv('DB_USER', 'allezulassungen')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'allezulassungen_pass')

    # Download
    DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', './data/raw')

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    # KBA Konfiguration
    KBA_BASE_URL = 'https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge'
    KBA_START_YEAR = 2008
    KBA_REQUEST_DELAY = 2  # Sekunden zwischen Requests (Rate Limiting)
    KBA_REQUEST_TIMEOUT = 30  # Timeout fuer HTTP-Requests
    KBA_USER_AGENT = (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    # Bilder-Speicherung
    # Physischer Pfad zum Bilder-Verzeichnis (wird im Backend's public/ abgelegt)
    _SCRAPPER_DIR = os.path.dirname(os.path.abspath(__file__))
    _PROJEKTE_DIR = os.path.dirname(_SCRAPPER_DIR)
    IMAGES_DIR = os.getenv('IMAGES_DIR', os.path.join(
        _PROJEKTE_DIR, 'allezulassungen Backend', 'public', 'images'
    ))
    # URL-Praefix fuer Bilder (wie in der DB gespeichert)
    IMAGES_BASE_URL = os.getenv('IMAGES_BASE_URL', '/images')

    # Scheduler
    SCHEDULER_CHECK_HOUR = 8  # Taeglich um 8 Uhr pruefen
    SCHEDULER_CHECK_MINUTE = 0
