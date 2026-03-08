"""
FZ9 Parser: Besitzumschreibungen (Eigentumswechsel / Gebrauchtwagenmarkt).

Die FZ9-Excel-Datei enthaelt:
- Zulassungsaenderungen bei Gebrauchtwagen
- Aehnliche Struktur wie FZ8, aber einfacher
- Aggregiert nach Fahrzeugart
"""
import logging

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ9Parser(BaseParser):
    """Parser fuer FZ9: Besitzumschreibungen nach Fahrzeugart."""

    QUELLE_KUERZEL = 'FZ9'

    SKIP_PATTERNS = [
        'insgesamt', 'zusammen', 'gesamt', 'summe',
        'sonstige', 'übrige', 'andere',
        'besitzumschreibungen', 'quelle:', 'stand:', 'datum:',
    ]

    def _is_skip_row(self, text):
        """Prueft ob eine Zeile uebersprungen werden soll."""
        if not text:
            return True
        lower = str(text).strip().lower()
        return any(pattern in lower for pattern in self.SKIP_PATTERNS)

    def _parse_sheet(self, sheet, sheet_name, year, month):
        """Parst ein einzelnes Sheet der FZ9-Datei."""
        results = []
        rows = list(sheet.iter_rows(values_only=True))

        if not rows:
            return results

        # Header-Zeile finden
        header_row = 0
        label_col = 0
        anzahl_col = None

        for i, row in enumerate(rows):
            if not row:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]
            for j, cell in enumerate(row_str):
                if any(h in cell for h in ['fahrzeugart', 'art', 'fahrzeuge', 'kategorie']):
                    label_col = j
                    header_row = i
                if any(h in cell for h in ['anzahl', 'besitzumschreibungen', 'wechsel', 'monat']):
                    anzahl_col = j

            if header_row == i:
                if anzahl_col is None:
                    for j in range(1, min(12, len(row))):
                        if j < len(row) and row[j] is not None:
                            try:
                                val = DataNormalizer.normalize_anzahl(row[j])
                                if isinstance(val, int) and val >= 0:
                                    anzahl_col = j
                                    break
                            except (ValueError, TypeError):
                                pass
                break

        if anzahl_col is None:
            anzahl_col = 1

        for i in range(header_row + 1, len(rows)):
            row = rows[i]
            if not row or len(row) <= max(label_col, anzahl_col):
                continue

            label = str(row[label_col]).strip() if row[label_col] else ''
            if not label or self._is_skip_row(label):
                continue

            anzahl = DataNormalizer.normalize_anzahl(
                row[anzahl_col] if anzahl_col < len(row) else None
            )
            if anzahl <= 0:
                continue

            results.append({
                'jahr': year,
                'monat': month,
                'fahrzeugart': label,
                'anzahl': anzahl,
            })

        return results

    def parse(self, filepath):
        """
        Parst eine FZ9 Excel-Datei.

        Returns:
            list: Liste von Dicts mit keys:
                jahr, monat, fahrzeugart, anzahl
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year:
            logger.error(f"Kann Jahr nicht bestimmen: {filepath}")
            self.close()
            return []

        results = []
        sheet_names = self.get_sheet_names()

        logger.info(f"FZ9 {year}/{month or '?'}: {len(sheet_names)} Sheets: {sheet_names}")

        for sheet_name in sheet_names:
            sheet = self.get_sheet(name=sheet_name)
            if not sheet:
                continue

            sheet_results = self._parse_sheet(sheet, sheet_name, year, month)
            results.extend(sheet_results)

        self.close()

        logger.info(f"FZ9 {year}/{month or '?'}: {len(results)} Datensaetze geparst")
        return results
