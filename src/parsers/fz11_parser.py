"""
FZ11 Parser: Neuzulassungen von PKW nach Segmenten und Modellreihen.

Die FZ11-Excel-Datei ist aehnlich zu FZ10, aber nach Segmenten gegliedert:
- Kleinwagen, Kompaktklasse, Mittelklasse, SUV, etc.
- Unter jedem Segment: Marke | Modell | Anzahl
- Aktueller Kontext (Segment) muss mitgefuehrt werden
"""
import logging

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ11Parser(BaseParser):
    """Parser fuer FZ11: PKW Neuzulassungen nach Segmenten und Modellreihen."""

    QUELLE_KUERZEL = 'FZ11'

    SKIP_PATTERNS = [
        'insgesamt', 'zusammen', 'gesamt', 'summe',
        'sonstige', 'übrige', 'andere',
        'neuzulassungen', 'personenkraftwagen',
        'kraftfahrt-bundesamt', 'statistik',
        'quelle:', 'stand:', 'datum:',
    ]

    # Bekannte Segment-Bezeichnungen
    SEGMENT_KEYWORDS = [
        'kleinwagen', 'kompaktklasse', 'mittelklasse', 'obere mittelklasse',
        'oberklasse', 'sportwagen', 'cabriolet', 'roadster',
        'suv', 'ssuv', 'suv-kompakt', 'suv-mittel', 'suv-gross',
        'minivan', 'van', 'kombi', 'stufenheck',
    ]

    def _is_skip_row(self, text):
        """Prueft ob eine Zeile uebersprungen werden soll."""
        if not text:
            return True
        lower = str(text).strip().lower()
        return any(pattern in lower for pattern in self.SKIP_PATTERNS)

    def _is_segment_header(self, text):
        """Prueft ob ein Text ein Segment-Header ist."""
        if not text or len(str(text).strip()) < 3:
            return False, None
        lower = str(text).strip().lower()
        for kw in self.SEGMENT_KEYWORDS:
            if kw in lower:
                return True, str(text).strip()
        # Auch: Zeilen die nur aus einem Segment-Namen bestehen (ohne Modell)
        if lower == lower.upper() and len(lower) > 4:
            return True, str(text).strip()
        return False, None

    def _parse_sheet(self, sheet, sheet_name, year, month):
        """Parst ein einzelnes Sheet der FZ11-Datei."""
        results = []
        current_segment = None
        rows = list(sheet.iter_rows(values_only=True))

        if not rows:
            return results

        # Header-Zeile finden
        header_row = None
        marke_col = 0
        modell_col = 1
        anzahl_col = None

        for i, row in enumerate(rows):
            if not row:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]
            for j, cell in enumerate(row_str):
                if any(h in cell for h in ['modell', 'modellreihe', 'handelsname']):
                    modell_col = j
                    header_row = i
                if any(h in cell for h in ['marke', 'hersteller']):
                    marke_col = j
                    header_row = i
                if any(h in cell for h in ['anzahl', 'neuzulassungen', 'monat']):
                    anzahl_col = j

            if header_row == i:
                if anzahl_col is None:
                    for j in range(max(marke_col, modell_col) + 1, min(len(row), 15)):
                        if row[j] is not None:
                            try:
                                int(DataNormalizer.normalize_anzahl(row[j]))
                                anzahl_col = j
                                break
                            except (ValueError, TypeError):
                                pass
                break

        if header_row is None:
            header_row = 0
        if anzahl_col is None:
            anzahl_col = max(marke_col, modell_col) + 1

        for i in range(header_row, len(rows)):
            row = rows[i]
            if not row:
                continue

            cells = [str(c).strip() if c is not None else '' for c in row]
            first_cell = cells[marke_col] if marke_col < len(cells) else ''
            second_cell = cells[modell_col] if modell_col < len(cells) else ''

            # Pruefen ob Segment-Header
            is_seg, seg_name = self._is_segment_header(first_cell)
            if is_seg:
                current_segment = DataNormalizer.normalize_modell(seg_name) or seg_name
                continue

            # Auch: Segment in zweiter Spalte oder alleinstehend
            if not current_segment and first_cell and not second_cell:
                is_seg2, seg_name2 = self._is_segment_header(first_cell)
                if is_seg2:
                    current_segment = DataNormalizer.normalize_modell(seg_name2) or seg_name2
                    continue

            if self._is_skip_row(first_cell) and self._is_skip_row(second_cell):
                continue

            # Marke/Modell-Zeile: Marke in Spalte 0, Modell in Spalte 1 (oder umgekehrt)
            marke_raw = first_cell if first_cell else None
            modell_raw = second_cell if second_cell else None

            # Fall: Nur eine Zelle gefuellt (z.B. Modell ohne Marke bei zusammengefuegten Zellen)
            if not modell_raw and marke_raw:
                modell_raw = marke_raw
                marke_raw = None

            marke = DataNormalizer.normalize_marke(marke_raw) if marke_raw else 'Unbekannt'
            modell = DataNormalizer.normalize_modell(modell_raw) if modell_raw else None

            if not modell:
                continue

            anzahl = DataNormalizer.normalize_anzahl(
                row[anzahl_col] if anzahl_col < len(row) else None
            )
            if anzahl <= 0:
                continue

            results.append({
                'marke': marke or 'Unbekannt',
                'modell': modell,
                'segment': current_segment,
                'jahr': year,
                'monat': month,
                'anzahl': anzahl,
            })

        return results

    def parse(self, filepath):
        """
        Parst eine FZ11 Excel-Datei.

        Returns:
            list: Liste von Dicts mit keys:
                marke, modell, segment, jahr, monat, anzahl
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year or not month:
            logger.error(f"Kann Jahr/Monat nicht bestimmen: {filepath}")
            self.close()
            return []

        results = []
        sheet_names = self.get_sheet_names()

        logger.info(f"FZ11 {year}/{month:02d}: {len(sheet_names)} Sheets: {sheet_names}")

        for sheet_name in sheet_names:
            sheet = self.get_sheet(name=sheet_name)
            if not sheet:
                continue

            sheet_results = self._parse_sheet(sheet, sheet_name, year, month)
            results.extend(sheet_results)

        self.close()

        logger.info(f"FZ11 {year}/{month:02d}: {len(results)} Datensaetze geparst")
        return results
