"""
FZ14 Parser: Neuzulassungen nach Umweltmerkmalen (CO2-Emissionen, Schadstoffklassen).
Jaehrliche Umweltdaten nach CO2-Klassen und Emissionsklassen.
"""
import logging
from pathlib import Path

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ14Parser(BaseParser):
    """Parser fuer FZ14: Neuzulassungen nach Umweltmerkmalen."""

    QUELLE_KUERZEL = 'FZ14'

    SKIP_PATTERNS = [
        'insgesamt', 'zusammen', 'gesamt', 'summe',
        'sonstige', 'übrige', 'andere',
        'quelle:', 'stand:', 'datum:',
    ]

    def _is_skip_row(self, text):
        """Prueft ob eine Zeile uebersprungen werden soll."""
        if not text:
            return True
        lower = str(text).strip().lower()
        return any(pattern in lower for pattern in self.SKIP_PATTERNS)

    def parse(self, filepath):
        """
        Parst eine FZ14 Excel-Datei (Umweltdaten).

        Returns:
            list: Liste von Dicts mit keys:
                jahr, monat (=0), fahrzeugart, kraftstoff, co2_klasse, emissionsklasse, anzahl
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year:
            logger.error(f"Kann Jahr nicht bestimmen: {filepath}")
            self.close()
            return []

        monat = 0  # Jaehrliche Daten
        results = []
        sheet_names = self.get_sheet_names()

        logger.info(f"FZ14 {year}: {len(sheet_names)} Sheets: {sheet_names}")

        for sheet_name in sheet_names:
            sheet = self.get_sheet(name=sheet_name)
            if not sheet:
                continue

            sheet_results = self._parse_sheet(sheet, sheet_name, year, monat)
            results.extend(sheet_results)

        self.close()

        logger.info(f"FZ14 {year}: {len(results)} Datensaetze geparst")
        return results

    def _parse_sheet(self, sheet, sheet_name, year, monat):
        """Parst ein einzelnes Sheet der FZ14-Datei."""
        results = []
        rows = list(sheet.iter_rows(values_only=True))

        if not rows:
            return results

        header_row = None
        label_col = 0
        anzahl_col = None
        co2_col = None
        emission_col = None
        fahrzeugart_col = None
        kraftstoff_col = None

        # Header-Zeile finden und Spalten identifizieren
        header_hints = ['co2', 'co₂', 'emission', 'schadstoff', 'fahrzeugart',
                       'kraftstoff', 'anzahl', 'neuzulassungen', 'zulassungen']
        for i, row in enumerate(rows):
            if not row:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]
            combined = ' '.join(row_str)

            for j, cell in enumerate(row_str):
                if any(h in cell for h in ['co2', 'co₂', 'co2-klasse', 'emissionsklasse']):
                    if 'emission' in cell or 'schadstoff' in cell:
                        emission_col = j
                    else:
                        co2_col = j
                elif any(h in cell for h in ['fahrzeugart', 'fz-art', 'art']):
                    fahrzeugart_col = j
                elif any(h in cell for h in ['kraftstoff', 'antrieb']):
                    kraftstoff_col = j
                elif any(h in cell for h in ['anzahl', 'neuzulassungen', 'zulassungen']):
                    anzahl_col = j

            if any(h in combined for h in header_hints):
                header_row = i
                break

        if header_row is None:
            header_row = 0

        if anzahl_col is None:
            for j in range(1, min(10, len(rows[header_row]) if header_row < len(rows) else 0)):
                if header_row + 1 < len(rows):
                    val = rows[header_row + 1][j] if j < len(rows[header_row + 1]) else None
                    if val is not None and DataNormalizer.normalize_anzahl(val) >= 0:
                        anzahl_col = j
                        break

        if anzahl_col is None:
            anzahl_col = max(
                (co2_col or 0), (emission_col or 0),
                (fahrzeugart_col or 0), (kraftstoff_col or 0)
            ) + 1

        # Daten parsen
        for i in range(header_row + 1, len(rows)):
            row = rows[i]
            if not row:
                continue

            label = str(row[label_col]).strip() if label_col < len(row) and row[label_col] else ''
            if not label or self._is_skip_row(label):
                continue

            anzahl = DataNormalizer.normalize_anzahl(
                row[anzahl_col] if anzahl_col < len(row) else None
            )
            if anzahl <= 0:
                continue

            record = {
                'jahr': year,
                'monat': monat,
                'fahrzeugart': None,
                'kraftstoff': None,
                'co2_klasse': None,
                'emissionsklasse': None,
                'anzahl': anzahl,
            }

            if fahrzeugart_col is not None and fahrzeugart_col < len(row) and row[fahrzeugart_col]:
                record['fahrzeugart'] = str(row[fahrzeugart_col]).strip()

            if kraftstoff_col is not None and kraftstoff_col < len(row) and row[kraftstoff_col]:
                record['kraftstoff'] = DataNormalizer.normalize_kraftstoff(
                    str(row[kraftstoff_col]).strip()
                )

            if co2_col is not None and co2_col < len(row) and row[co2_col]:
                record['co2_klasse'] = str(row[co2_col]).strip()

            if emission_col is not None and emission_col < len(row) and row[emission_col]:
                record['emissionsklasse'] = str(row[emission_col]).strip()

            if record['fahrzeugart'] is None and record['kraftstoff'] is None:
                norm_kraftstoff = DataNormalizer.normalize_kraftstoff(label)
                if norm_kraftstoff and norm_kraftstoff != 'Insgesamt':
                    record['kraftstoff'] = norm_kraftstoff
                else:
                    record['fahrzeugart'] = label

            results.append(record)

        return results
