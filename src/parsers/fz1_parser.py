"""
FZ1 Parser: Fahrzeugbestand zum 1. Januar jedes Jahres.
Nicht Neuzulassungen, sondern der Bestand aller zugelassenen Fahrzeuge.
"""
import logging
from pathlib import Path

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ1Parser(BaseParser):
    """Parser fuer FZ1: Fahrzeugbestand (Bestandsstatistik)."""

    QUELLE_KUERZEL = 'FZ1'

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
        Parst eine FZ1 Excel-Datei (Fahrzeugbestand).

        Returns:
            list: Liste von Dicts mit keys:
                marke, jahr, anzahl, kraftstoff, fahrzeugart
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year:
            logger.error(f"Kann Jahr nicht bestimmen: {filepath}")
            self.close()
            return []

        results = []
        sheet_names = self.get_sheet_names()

        logger.info(f"FZ1 {year}: {len(sheet_names)} Sheets: {sheet_names}")

        for sheet_name in sheet_names:
            sheet = self.get_sheet(name=sheet_name)
            if not sheet:
                continue

            sheet_results = self._parse_sheet(sheet, sheet_name, year)
            results.extend(sheet_results)

        self.close()

        logger.info(f"FZ1 {year}: {len(results)} Datensaetze geparst")
        return results

    def _parse_sheet(self, sheet, sheet_name, year):
        """Parst ein einzelnes Sheet der FZ1-Datei."""
        results = []
        rows = list(sheet.iter_rows(values_only=True))

        if not rows:
            return results

        kraftstoff = self._detect_kraftstoff_from_sheet(sheet_name)
        fahrzeugart = self._detect_fahrzeugart_from_sheet(sheet_name)

        header_row = None
        marke_col = 0
        anzahl_col = None
        kraftstoff_col = None
        fahrzeugart_col = None

        # Header-Zeile finden
        for i, row in enumerate(rows):
            if not row:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]

            for j, cell in enumerate(row_str):
                if any(h in cell for h in ['marke', 'hersteller', 'hersteller/marke']):
                    marke_col = j
                    header_row = i
                elif any(h in cell for h in ['kraftstoff', 'antrieb']):
                    kraftstoff_col = j
                elif any(h in cell for h in ['fahrzeugart', 'fz-art', 'art']):
                    fahrzeugart_col = j
                elif any(h in cell for h in ['anzahl', 'bestand', 'zugelassen']):
                    anzahl_col = j

            if header_row == i:
                if anzahl_col is None:
                    for j in range(marke_col + 1, min(marke_col + 5, len(row))):
                        if j < len(row) and row[j] is not None:
                            try:
                                int(str(row[j]).replace('.', '').replace(',', ''))
                                anzahl_col = j
                                break
                            except (ValueError, AttributeError):
                                continue
                break

        if header_row is None:
            header_row = 0

        if anzahl_col is None:
            anzahl_col = marke_col + 1

        # Daten parsen
        for i in range(header_row + 1, len(rows)):
            row = rows[i]
            if not row or len(row) <= max(marke_col, anzahl_col):
                continue

            marke_raw = str(row[marke_col]).strip() if marke_col < len(row) and row[marke_col] else ''
            if not marke_raw or self._is_skip_row(marke_raw):
                continue

            marke = DataNormalizer.normalize_marke(marke_raw)
            if not marke:
                continue

            anzahl = DataNormalizer.normalize_anzahl(
                row[anzahl_col] if anzahl_col < len(row) else None
            )
            if anzahl <= 0:
                continue

            record = {
                'marke': marke,
                'jahr': year,
                'anzahl': anzahl,
                'kraftstoff': kraftstoff,
                'fahrzeugart': fahrzeugart,
            }

            if kraftstoff_col is not None and kraftstoff_col < len(row) and row[kraftstoff_col]:
                record['kraftstoff'] = DataNormalizer.normalize_kraftstoff(
                    str(row[kraftstoff_col]).strip()
                )

            if fahrzeugart_col is not None and fahrzeugart_col < len(row) and row[fahrzeugart_col]:
                record['fahrzeugart'] = str(row[fahrzeugart_col]).strip()

            results.append(record)

        return results

    def _detect_kraftstoff_from_sheet(self, sheet_name):
        """Erkennt den Kraftstofftyp aus dem Sheet-Namen."""
        name_lower = (sheet_name or '').lower()

        mapping = {
            'benzin': 'Benzin',
            'otto': 'Benzin',
            'diesel': 'Diesel',
            'elektro': 'Elektro',
            'bev': 'Elektro',
            'hybrid': 'Hybrid',
            'plug-in': 'Plug-in-Hybrid',
            'erdgas': 'Erdgas/CNG',
            'cng': 'Erdgas/CNG',
            'lpg': 'Autogas/LPG',
            'autogas': 'Autogas/LPG',
        }

        for key, value in mapping.items():
            if key in name_lower:
                return value

        return None

    def _detect_fahrzeugart_from_sheet(self, sheet_name):
        """Erkennt die Fahrzeugart aus dem Sheet-Namen."""
        name_lower = (sheet_name or '').lower()

        if 'pkw' in name_lower or 'personenkraftwagen' in name_lower:
            return 'PKW'
        if 'lkw' in name_lower or 'lastkraftwagen' in name_lower:
            return 'LKW'
        if 'kraftrad' in name_lower or 'motorrad' in name_lower:
            return 'Kraftrad'
        if 'bus' in name_lower:
            return 'Bus'
        if 'wohnmobil' in name_lower:
            return 'Wohnmobil'

        return None
