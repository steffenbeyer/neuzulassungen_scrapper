"""
ACEA/EZB Parser: Europaeische Fahrzeug-Neuzulassungsdaten.

Datenquelle: European Central Bank (ECB) CAR Dataset
URL: https://data.ecb.europa.eu/data/datasets/CAR
Format: CSV
Inhalt: Monatliche PKW-Neuzulassungen pro Land (EU + CH/NO/UK), ab 1990

Die CSV-Datei hat folgende Struktur:
- Spalten fuer verschiedene Laender (ISO-Codes)
- Zeilen fuer jeden Monat (Format: YYYY-MM)
- Werte: Anzahl Neuzulassungen
"""
import csv
import logging
from pathlib import Path
from datetime import datetime

from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)

# Mapping ECB-Laendercodes zu ISO-Codes
ECB_COUNTRY_MAP = {
    'AT': 'AT',  # Oesterreich
    'BE': 'BE',  # Belgien
    'BG': 'BG',  # Bulgarien
    'HR': 'HR',  # Kroatien
    'CY': 'CY',  # Zypern
    'CZ': 'CZ',  # Tschechien
    'DK': 'DK',  # Daenemark
    'EE': 'EE',  # Estland
    'FI': 'FI',  # Finnland
    'FR': 'FR',  # Frankreich
    'DE': 'DE',  # Deutschland
    'GR': 'GR',  # Griechenland
    'HU': 'HU',  # Ungarn
    'IE': 'IE',  # Irland
    'IT': 'IT',  # Italien
    'LV': 'LV',  # Lettland
    'LT': 'LT',  # Litauen
    'LU': 'LU',  # Luxemburg
    'MT': 'MT',  # Malta
    'NL': 'NL',  # Niederlande
    'PL': 'PL',  # Polen
    'PT': 'PT',  # Portugal
    'RO': 'RO',  # Rumaenien
    'SK': 'SK',  # Slowakei
    'SI': 'SI',  # Slowenien
    'ES': 'ES',  # Spanien
    'SE': 'SE',  # Schweden
    'NO': 'NO',  # Norwegen
    'CH': 'CH',  # Schweiz
    'GB': 'GB',  # Grossbritannien
    'UK': 'GB',  # UK Alias
}

# Laendernamen
COUNTRY_NAMES = {
    'AT': 'Österreich', 'BE': 'Belgien', 'BG': 'Bulgarien',
    'HR': 'Kroatien', 'CY': 'Zypern', 'CZ': 'Tschechien',
    'DK': 'Dänemark', 'EE': 'Estland', 'FI': 'Finnland',
    'FR': 'Frankreich', 'DE': 'Deutschland', 'GR': 'Griechenland',
    'HU': 'Ungarn', 'IE': 'Irland', 'IT': 'Italien',
    'LV': 'Lettland', 'LT': 'Litauen', 'LU': 'Luxemburg',
    'MT': 'Malta', 'NL': 'Niederlande', 'PL': 'Polen',
    'PT': 'Portugal', 'RO': 'Rumänien', 'SK': 'Slowakei',
    'SI': 'Slowenien', 'ES': 'Spanien', 'SE': 'Schweden',
    'NO': 'Norwegen', 'CH': 'Schweiz', 'GB': 'Großbritannien',
}


class ACEAParser:
    """Parser fuer ACEA/EZB Neuzulassungsdaten im CSV-Format."""

    QUELLE_KUERZEL = 'ACEA_CAR'

    def parse(self, filepath):
        """
        Parst eine ACEA/EZB CSV-Datei.

        Returns:
            list: Liste von Dicts mit keys:
                - land_code: ISO-Laendercode
                - land_name: Laendername
                - jahr: int
                - monat: int
                - anzahl: int
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Datei nicht gefunden: {filepath}")

        logger.info(f"Parse ACEA-Daten: {filepath.name}")
        results = []

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                parsed = self._parse_row(row)
                if parsed:
                    results.extend(parsed)

        logger.info(f"ACEA: {len(results)} Datensaetze geparst")
        return results

    def _parse_row(self, row):
        """Parst eine einzelne CSV-Zeile."""
        results = []

        # Zeitperiode extrahieren (Format: YYYY-MM oder YYYY)
        period = row.get('TIME_PERIOD', row.get('date', row.get('period', '')))
        if not period:
            return results

        try:
            if '-' in period:
                parts = period.split('-')
                year = int(parts[0])
                month = int(parts[1])
            else:
                year = int(period)
                month = 0
        except (ValueError, IndexError):
            return results

        # Fuer jedes Land den Wert extrahieren
        for key, value in row.items():
            # Laendercode aus Spaltenname extrahieren
            country_code = key.strip().upper()

            if country_code in ECB_COUNTRY_MAP:
                iso_code = ECB_COUNTRY_MAP[country_code]
                anzahl = DataNormalizer.normalize_anzahl(value)

                if anzahl > 0:
                    results.append({
                        'land_code': iso_code,
                        'land_name': COUNTRY_NAMES.get(iso_code, iso_code),
                        'jahr': year,
                        'monat': month,
                        'anzahl': anzahl,
                    })

        return results

    def parse_ecb_format(self, filepath):
        """
        Parst das spezifische ECB-Datenformat (Bulk-Download).
        ECB-Dateien haben oft ein anderes Format mit Metadaten-Spalten.

        Returns:
            list: Wie parse(), aber angepasst an ECB-Spaltenstruktur
        """
        filepath = Path(filepath)
        results = []

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                # ECB-Format hat Spalten wie:
                # KEY, FREQ, REF_AREA, UNIT, OBS_VALUE, TIME_PERIOD
                ref_area = row.get('REF_AREA', '').strip().upper()
                obs_value = row.get('OBS_VALUE', '')
                time_period = row.get('TIME_PERIOD', '')

                if ref_area not in ECB_COUNTRY_MAP:
                    continue

                iso_code = ECB_COUNTRY_MAP[ref_area]

                try:
                    if '-' in time_period:
                        parts = time_period.split('-')
                        year = int(parts[0])
                        month = int(parts[1])
                    else:
                        year = int(time_period)
                        month = 0
                except (ValueError, IndexError):
                    continue

                anzahl = DataNormalizer.normalize_anzahl(obs_value)
                if anzahl > 0:
                    results.append({
                        'land_code': iso_code,
                        'land_name': COUNTRY_NAMES.get(iso_code, iso_code),
                        'jahr': year,
                        'monat': month,
                        'anzahl': anzahl,
                    })

        logger.info(f"ACEA (ECB-Format): {len(results)} Datensaetze geparst")
        return results
