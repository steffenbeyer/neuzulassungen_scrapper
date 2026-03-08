#!/usr/bin/env python3
"""
AlleZulassungen Scrapper - Hauptprogramm
=========================================
Laedt KFZ-Neuzulassungsdaten vom KBA (Kraftfahrt-Bundesamt) herunter,
parst die Excel-Dateien und speichert die Daten in MariaDB.

Usage:
    python main.py --mode initial     # Initialer Import aller verfuegbaren Daten
    python main.py --mode update      # Nur neue Daten pruefen und importieren
    python main.py --mode download    # Nur herunterladen, nicht importieren
    python main.py --mode scheduler   # Dauerhaft laufen, taeglich pruefen
    python main.py --mode parse-file --file <pfad>  # Einzelne Datei parsen
"""
import argparse
import logging
import sys
from pathlib import Path

from config import Config
from src.database import db
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
from src.scheduler import DataScheduler
from src.acea_importer import ACEAImporter
from src.wikidata_importer import WikidataImporter
from src.openev_importer import OpenEVImporter
from src.llm_enricher import LLMEnricher
from src.fuel_mapper import FuelMapper


def setup_logging():
    """Konfiguriert das Logging."""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('scrapper.log', encoding='utf-8'),
        ]
    )


def run_initial_import():
    """
    Fuehrt den initialen Import durch:
    1. Alle verfuegbaren FZ10-Dateien herunterladen (2008-heute)
    2. Alle Dateien parsen und in die Datenbank schreiben
    """
    logger = logging.getLogger('initial_import')
    logger.info("========================================")
    logger.info("INITIALER IMPORT GESTARTET")
    logger.info("========================================")

    downloader = KBADownloader()
    parser = FZ10Parser()
    writer = DBWriter()

    # 1. FZ10 herunterladen (Kern-Datensatz)
    logger.info("--- Phase 1: FZ10 herunterladen ---")
    downloaded = downloader.download_monthly_range('FZ10')
    logger.info(f"{len(downloaded)} neue FZ10-Dateien heruntergeladen")

    # 2. Alle vorhandenen FZ10-Dateien parsen
    logger.info("--- Phase 2: FZ10 parsen und importieren ---")
    fz10_dir = Path(Config.DOWNLOAD_DIR) / 'fz10'

    if not fz10_dir.exists():
        logger.warning(f"Verzeichnis nicht gefunden: {fz10_dir}")
        return

    files = sorted(fz10_dir.glob('*.xlsx'))
    logger.info(f"{len(files)} FZ10-Dateien gefunden")

    total_rows = 0
    for filepath in files:
        filename = filepath.name

        if writer.is_already_imported('FZ10', filename):
            logger.debug(f"Bereits importiert: {filename}")
            continue

        try:
            # Jahr und Monat aus Dateiname
            parser_instance = FZ10Parser()
            parser_instance.load(filepath)
            year, month = parser_instance.extract_year_month_from_filename()
            parser_instance.close()

            writer.log_import('FZ10', filename, year, month, 'running')

            # Parsen
            data = parser.parse(filepath)

            if not data:
                logger.warning(f"Keine Daten geparst: {filename}")
                writer.log_import('FZ10', filename, year, month, 'error',
                                   error_message='Keine Daten geparst')
                continue

            # In DB schreiben
            rows = writer.write_neuzulassungen(data, quelle_kuerzel='FZ10')
            total_rows += rows

            writer.log_import('FZ10', filename, year, month, 'success', rows)
            logger.info(f"Importiert: {filename} -> {rows} Datensaetze")

        except Exception as e:
            logger.error(f"Fehler bei {filename}: {e}", exc_info=True)
            writer.log_import('FZ10', filename, year, month, 'error',
                               error_message=str(e))

    logger.info("========================================")
    logger.info(f"INITIALER IMPORT ABGESCHLOSSEN: {total_rows} Datensaetze")
    logger.info("========================================")


def run_update():
    """Prueft auf neue Daten und importiert sie."""
    logger = logging.getLogger('update')
    logger.info("Update-Modus: Pruefe auf neue Daten...")

    scheduler = DataScheduler()
    scheduler.check_and_import()


def run_download_only():
    """Laedt nur Dateien herunter, ohne Import."""
    logger = logging.getLogger('download')
    logger.info("Download-Modus: Lade alle verfuegbaren Dateien herunter...")

    downloader = KBADownloader()

    # Monatliche Daten
    for pub_type in KBADownloader.MONTHLY_TYPES:
        logger.info(f"--- {pub_type} ---")
        downloader.download_monthly_range(pub_type)

    # Jaehrliche Daten
    for pub_type in KBADownloader.YEARLY_TYPES:
        logger.info(f"--- {pub_type} ---")
        downloader.download_yearly_range(pub_type)


def run_scheduler():
    """Startet den dauerhaften Scheduler."""
    logger = logging.getLogger('scheduler')
    logger.info("Scheduler-Modus: Starte taegeliche Pruefung...")

    scheduler = DataScheduler()
    scheduler.start()


def run_full_import():
    """Importiert ALLE verfuegbaren Datenquellen (KBA + ACEA)."""
    logger = logging.getLogger('full_import')
    logger.info("========================================")
    logger.info("VOLLSTAENDIGER IMPORT GESTARTET")
    logger.info("========================================")

    downloader = KBADownloader()
    writer = DBWriter()

    # Monatliche Parser und ihre Konfiguration
    monthly_parsers = {
        'FZ10': FZ10Parser(),
        'FZ11': FZ11Parser(),
        'FZ8':  FZ8Parser(),
        'FZ9':  FZ9Parser(),
        'FZ28': FZ28Parser(),
    }

    # Jaehrliche Parser
    yearly_parsers = {
        'FZ4':  FZ4Parser(),
        'FZ14': FZ14Parser(),
        'FZ1':  FZ1Parser(),
    }

    # 1. Monatliche Daten herunterladen und importieren
    for pub_type, parser in monthly_parsers.items():
        logger.info(f"--- {pub_type}: Download ---")
        downloader.download_monthly_range(pub_type)

        logger.info(f"--- {pub_type}: Import ---")
        data_dir = Path(Config.DOWNLOAD_DIR) / pub_type.lower()
        if not data_dir.exists():
            continue

        for filepath in sorted(data_dir.glob('*.xlsx')):
            if writer.is_already_imported(pub_type, filepath.name):
                continue
            try:
                data = parser.parse(filepath)
                if data:
                    if pub_type in ('FZ8', 'FZ9'):
                        rows = writer.write_aggregiert(data, quelle_kuerzel=pub_type)
                    else:
                        rows = writer.write_neuzulassungen(data, quelle_kuerzel=pub_type)
                    writer.log_import(pub_type, filepath.name, None, None, 'success', rows)
                    logger.info(f"  {filepath.name}: {rows} Datensaetze")
            except Exception as e:
                logger.error(f"  Fehler {filepath.name}: {e}")
                writer.log_import(pub_type, filepath.name, None, None, 'error', error_message=str(e))

    # 2. Jaehrliche Daten herunterladen und importieren
    for pub_type, parser in yearly_parsers.items():
        logger.info(f"--- {pub_type}: Download ---")
        downloader.download_yearly_range(pub_type)

        logger.info(f"--- {pub_type}: Import ---")
        data_dir = Path(Config.DOWNLOAD_DIR) / pub_type.lower()
        if not data_dir.exists():
            continue

        for filepath in sorted(data_dir.glob('*.xlsx')):
            if writer.is_already_imported(pub_type, filepath.name):
                continue
            try:
                data = parser.parse(filepath)
                if data:
                    if pub_type == 'FZ1':
                        # Bestandsdaten separat behandeln
                        rows = _write_bestand(writer, data)
                    elif pub_type in ('FZ8', 'FZ14'):
                        rows = writer.write_aggregiert(data, quelle_kuerzel=pub_type)
                    else:
                        rows = writer.write_neuzulassungen(data, quelle_kuerzel=pub_type)
                    writer.log_import(pub_type, filepath.name, None, None, 'success', rows)
                    logger.info(f"  {filepath.name}: {rows} Datensaetze")
            except Exception as e:
                logger.error(f"  Fehler {filepath.name}: {e}")
                writer.log_import(pub_type, filepath.name, None, None, 'error', error_message=str(e))

    # 3. ACEA-Daten
    logger.info("--- ACEA: Import ---")
    run_acea_import()

    logger.info("========================================")
    logger.info("VOLLSTAENDIGER IMPORT ABGESCHLOSSEN")
    logger.info("========================================")


def _write_bestand(writer, data):
    """Schreibt Fahrzeugbestand-Daten (FZ1) in die DB."""
    from src.normalizer import DataNormalizer
    land_id = db.get_land_id('DE')
    quelle_id = db.get_quelle_id('FZ1')
    written = 0

    for row in data:
        try:
            marke_id = writer.get_or_create_marke(row.get('marke')) if row.get('marke') else None
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
            logging.getLogger('bestand').error(f"Fehler: {e}")

    return written


def run_acea_import():
    """Importiert ACEA/EZB europaeische Zulassungsdaten."""
    logger = logging.getLogger('acea_import')
    logger.info("ACEA/EZB Import gestartet...")

    importer = ACEAImporter()
    results = importer.import_data()

    if results:
        total = sum(results.values())
        logger.info(f"ACEA Import: {total} Datensaetze aus {len(results)} Laendern")
    else:
        logger.info("ACEA Import: Keine Daten importiert")


def run_enrich_marken(force=False):
    """Reichert Marken-Stammdaten mit Wikidata/Wikipedia-Daten an."""
    logger = logging.getLogger('enrich_marken')
    logger.info("========================================")
    logger.info("WIKIDATA MARKEN-ANREICHERUNG GESTARTET")
    logger.info("========================================")

    importer = WikidataImporter()
    stats = importer.enrich_marken(force=force)

    logger.info(
        f"Fertig: {stats['enriched']} angereichert, "
        f"{stats['not_found']} nicht gefunden, "
        f"{stats['errors']} Fehler"
    )


def run_enrich_modelle(force=False):
    """Reichert Modell-Stammdaten mit Wikidata/Wikipedia-Daten an."""
    logger = logging.getLogger('enrich_modelle')
    logger.info("========================================")
    logger.info("WIKIDATA MODELL-ANREICHERUNG GESTARTET")
    logger.info("========================================")

    importer = WikidataImporter()
    stats = importer.enrich_modelle(force=force)

    logger.info(
        f"Fertig: {stats['enriched']} angereichert, "
        f"{stats['not_found']} nicht gefunden, "
        f"{stats['errors']} Fehler"
    )


def run_import_openev():
    """Importiert E-Fahrzeug-Spezifikationen aus OpenEV Data."""
    logger = logging.getLogger('import_openev')
    logger.info("========================================")
    logger.info("OPENEV DATA IMPORT GESTARTET")
    logger.info("========================================")

    importer = OpenEVImporter()
    stats = importer.download_and_import()

    if stats:
        logger.info(
            f"Fertig: {stats['imported']} importiert, "
            f"{stats['updated']} aktualisiert, "
            f"{stats['skipped_brand']} Marke nicht gefunden, "
            f"{stats['skipped_model']} Modell nicht gefunden, "
            f"{stats['errors']} Fehler"
        )
    else:
        logger.error("Import fehlgeschlagen")


def run_fix_logos():
    """Repariert Marken-Logos mit verifizierten Wikidata-QIDs."""
    logger = logging.getLogger('fix_logos')
    logger.info("========================================")
    logger.info("LOGO-REPARATUR GESTARTET")
    logger.info("========================================")

    importer = WikidataImporter()
    stats = importer.fix_brand_logos()

    logger.info(
        f"Fertig: {stats['fixed']} Logos heruntergeladen, "
        f"{stats['no_logo']} ohne Logo, "
        f"{stats['errors']} Fehler"
    )


def run_llm_enrich_marken(force=False):
    """Reichert Marken-Stammdaten mit LLM-generierten Daten an."""
    logger = logging.getLogger('llm_enrich_marken')
    logger.info("========================================")
    logger.info("LLM MARKEN-ANREICHERUNG GESTARTET")
    logger.info("========================================")

    enricher = LLMEnricher()
    stats = enricher.enrich_marken(force=force)

    logger.info(
        f"Fertig: {stats['enriched']} angereichert, "
        f"{stats['errors']} Fehler"
    )


def run_llm_enrich_modelle(force=False):
    """Reichert Modell-Stammdaten mit LLM-generierten Daten an."""
    logger = logging.getLogger('llm_enrich_modelle')
    logger.info("========================================")
    logger.info("LLM MODELL-ANREICHERUNG GESTARTET")
    logger.info("========================================")

    enricher = LLMEnricher()
    stats = enricher.enrich_modelle(force=force)

    logger.info(
        f"Fertig: {stats['enriched']} angereichert, "
        f"{stats['errors']} Fehler"
    )


def run_fuel_map(year=None, month=None):
    """Berechnet geschaetzte Kraftstoff-Verteilungen aus FZ28+FZ10."""
    logger = logging.getLogger('fuel_map')
    logger.info("========================================")
    logger.info("KRAFTSTOFF-MAPPING GESTARTET")
    logger.info("========================================")

    mapper = FuelMapper()
    stats = mapper.run(year=year, month=month)

    logger.info(
        f"Fertig: {stats['mapped']} Datensaetze geschrieben, "
        f"{stats['skipped_no_fz28']} ohne FZ28, "
        f"{stats['skipped_no_fz10']} ohne FZ10, "
        f"{stats['errors']} Fehler"
    )


def run_parse_file(filepath):
    """Parst eine einzelne Datei (fuer Tests/Debugging)."""
    logger = logging.getLogger('parse_file')
    filepath = Path(filepath)

    if not filepath.exists():
        logger.error(f"Datei nicht gefunden: {filepath}")
        return

    logger.info(f"Parse Datei: {filepath}")

    # Typ erkennen
    stem = filepath.stem.lower()
    if stem.startswith('fz10'):
        parser = FZ10Parser()
    else:
        logger.error(f"Unbekannter Dateityp: {filepath.name}")
        logger.info("Unterstuetzte Typen: fz10_*.xlsx")
        return

    data = parser.parse(filepath)

    if not data:
        logger.warning("Keine Daten geparst!")
        return

    logger.info(f"{len(data)} Datensaetze geparst:")
    # Erste 20 Datensaetze anzeigen
    for row in data[:20]:
        logger.info(
            f"  {row['marke']:20s} | {row['modell']:30s} | "
            f"{row['anzahl']:>8d} | {row.get('kraftstoff', '-')}"
        )

    if len(data) > 20:
        logger.info(f"  ... und {len(data) - 20} weitere")

    # Optional: In DB schreiben
    writer = DBWriter()
    rows = writer.write_neuzulassungen(data, quelle_kuerzel=parser.QUELLE_KUERZEL)
    logger.info(f"{rows} Datensaetze in DB geschrieben")


def main():
    """Hauptprogramm."""
    setup_logging()
    logger = logging.getLogger('main')

    parser = argparse.ArgumentParser(
        description='AlleZulassungen Scrapper - KBA Daten herunterladen und importieren'
    )
    parser.add_argument(
        '--mode',
        choices=[
            'initial', 'update', 'download', 'scheduler', 'parse-file',
            'import-all', 'import-acea', 'import-openev',
            'enrich-marken', 'enrich-modelle', 'enrich-all',
            'llm-enrich-marken', 'llm-enrich-modelle', 'llm-enrich-all',
            'fix-logos', 'fuel-map'
        ],
        default='initial',
        help='Betriebsmodus (default: initial)'
    )
    parser.add_argument(
        '--file',
        help='Dateipfad fuer parse-file Modus'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Erzwingt erneute Verarbeitung (z.B. bei enrich-marken)'
    )

    args = parser.parse_args()

    # Datenbankverbindung herstellen
    try:
        db.connect()
        logger.info("Datenbankverbindung hergestellt.")
    except Exception as e:
        logger.error(f"Kann keine Datenbankverbindung herstellen: {e}")
        if args.mode != 'download':
            sys.exit(1)

    try:
        if args.mode == 'initial':
            run_initial_import()
        elif args.mode == 'update':
            run_update()
        elif args.mode == 'download':
            run_download_only()
        elif args.mode == 'scheduler':
            run_scheduler()
        elif args.mode == 'parse-file':
            if not args.file:
                logger.error("--file Parameter erforderlich fuer parse-file Modus")
                sys.exit(1)
            run_parse_file(args.file)
        elif args.mode == 'import-all':
            run_full_import()
        elif args.mode == 'import-acea':
            run_acea_import()
        elif args.mode == 'import-openev':
            run_import_openev()
        elif args.mode == 'enrich-marken':
            run_enrich_marken(force=args.force)
        elif args.mode == 'enrich-modelle':
            run_enrich_modelle(force=args.force)
        elif args.mode == 'enrich-all':
            run_enrich_marken(force=args.force)
            run_enrich_modelle(force=args.force)
        elif args.mode == 'llm-enrich-marken':
            run_llm_enrich_marken(force=args.force)
        elif args.mode == 'llm-enrich-modelle':
            run_llm_enrich_modelle(force=args.force)
        elif args.mode == 'llm-enrich-all':
            run_llm_enrich_marken(force=args.force)
            run_llm_enrich_modelle(force=args.force)
        elif args.mode == 'fix-logos':
            run_fix_logos()
        elif args.mode == 'fuel-map':
            run_fuel_map()
    except KeyboardInterrupt:
        logger.info("Abgebrochen durch Benutzer.")
    except Exception as e:
        logger.error(f"Unerwarteter Fehler: {e}", exc_info=True)
        sys.exit(1)
    finally:
        db.close()


if __name__ == '__main__':
    main()
