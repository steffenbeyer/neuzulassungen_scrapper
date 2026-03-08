"""
FZ11 Parser: Neuzulassungen von PKW nach Segmenten und Modellreihen.

FZ11 Excel structure (data sheet FZ11.1):
- Row 8: Header: Segment | Modellreihe | (leer) | Anzahl | Anteil | ...
- Row 9+: Data rows
  - Segment name appears in col B only on the first row of each segment
  - Model name (combined "MARKE MODELL") in col C
  - Count in col E (as string)
  - Segment total rows: "MINIS ZUSAMMEN" in col B with no model
"""
import logging
import re

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ11Parser(BaseParser):
    """Parser fuer FZ11: PKW Neuzulassungen nach Segmenten und Modellreihen."""

    QUELLE_KUERZEL = 'FZ11'

    SKIP_PATTERNS = [
        'insgesamt', 'zusammen', 'sonstige',
        'quelle:', 'stand:', 'datum:',
        'kraftfahrt-bundesamt', 'zurück',
    ]

    # Known segment names in FZ11
    KNOWN_SEGMENTS = [
        'minis', 'kleinwagen', 'kompaktklasse', 'mittelklasse',
        'obere mittelklasse', 'oberklasse', 'suvs', 'suv',
        'geländewagen', 'sportwagen', 'mini-vans', 'grossraum-vans',
        'utilities', 'wohnmobile', 'cabriolets',
    ]

    def _is_skip_row(self, text):
        """Prueft ob eine Zeile uebersprungen werden soll."""
        if not text:
            return True
        lower = str(text).strip().lower()
        return any(pattern in lower for pattern in self.SKIP_PATTERNS)

    def _is_segment_name(self, text):
        """Prueft ob ein Text ein Segment-Name ist."""
        if not text:
            return False
        lower = str(text).strip().lower()
        return any(seg in lower for seg in self.KNOWN_SEGMENTS)

    def _split_brand_model(self, combined):
        """
        Splittet einen kombinierten 'MARKE MODELL' String.
        Z.B. 'DACIA SPRING' -> ('Dacia', 'Spring')
              'LAND ROVER DEFENDER' -> ('Land Rover', 'Defender')
              'MG ROEWE 3' -> ('MG Roewe', '3')
        """
        if not combined:
            return None, None

        text = str(combined).strip()
        if not text:
            return None, None

        # Known multi-word brands
        multi_word_brands = [
            'ALFA ROMEO', 'ASTON MARTIN', 'LAND ROVER', 'ROLLS ROYCE',
            'MG ROEWE', 'DS ', 'LYNK & CO', 'GREAT WALL',
        ]

        text_upper = text.upper()
        for brand in multi_word_brands:
            if text_upper.startswith(brand):
                brand_part = text[:len(brand)].strip()
                model_part = text[len(brand):].strip()
                if model_part:
                    return brand_part, model_part
                return brand_part, brand_part  # Brand-only

        # Default: first word is brand, rest is model
        parts = text.split(None, 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return parts[0], parts[0]  # Single word = brand is model

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

        # Find the data sheet (FZ11.1 or similar)
        data_sheet_name = None
        for name in sheet_names:
            if '11' in name and name not in ('Deckblatt', 'Impressum', 'Inhaltsverzeichnis'):
                data_sheet_name = name
                break

        if not data_sheet_name:
            logger.warning(f"FZ11 {year}/{month:02d}: Kein Daten-Sheet gefunden in {sheet_names}")
            self.close()
            return []

        sheet = self.get_sheet(name=data_sheet_name)
        if not sheet:
            self.close()
            return []

        rows = list(sheet.iter_rows(values_only=True))
        self.close()

        # Find header row (contains 'Segment' and 'Modellreihe')
        header_idx = None
        modell_col = 2  # Default: column C
        anzahl_col = 4  # Default: column E

        for i, row in enumerate(rows):
            if not row or len(row) < 3:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]
            # Look for header row where individual cells are 'Segment' and 'Modellreihe'
            # (not substring matches in long title texts)
            has_segment_col = any(cell.strip() == 'segment' for cell in row_str)
            has_modell_col = any('modellreihe' in cell for cell in row_str if len(cell) < 30)
            if has_segment_col and has_modell_col:
                header_idx = i
                anzahl_found = False
                for j, cell in enumerate(row_str):
                    if 'modellreihe' in cell:
                        modell_col = j
                    if cell.strip() == 'anzahl' and not anzahl_found:
                        anzahl_col = j
                        anzahl_found = True
                break

        if header_idx is None:
            # Fallback: look for first row with 'Anzahl'
            for i, row in enumerate(rows):
                if not row:
                    continue
                row_str = [str(c).strip().lower() if c else '' for c in row]
                if any('anzahl' in cell for cell in row_str):
                    header_idx = i
                    for j, cell in enumerate(row_str):
                        if 'anzahl' in cell:
                            anzahl_col = j
                            break
                    break

        if header_idx is None:
            logger.warning(f"FZ11 {year}/{month:02d}: Kein Header gefunden")
            return []

        # Parse data rows
        current_segment = None
        segment_col = 1  # Column B

        for i in range(header_idx + 1, len(rows)):
            row = rows[i]
            if not row or len(row) <= anzahl_col:
                continue

            segment_cell = str(row[segment_col]).strip() if segment_col < len(row) and row[segment_col] else ''
            modell_cell = str(row[modell_col]).strip() if modell_col < len(row) and row[modell_col] else ''

            # Check for copyright / end marker
            combined_text = (segment_cell + ' ' + modell_cell).lower()
            if '©' in combined_text or 'kraftfahrt-bundesamt' in combined_text:
                break

            # Update segment if present in column B
            if segment_cell and self._is_segment_name(segment_cell):
                current_segment = segment_cell
                # Segment row may also have first model in modell_col
                # Don't skip — fall through to parse the model if present

            # Skip total rows ("MINIS ZUSAMMEN", "INSGESAMT", etc.)
            if segment_cell and self._is_skip_row(segment_cell):
                continue
            if segment_cell and 'zusammen' in segment_cell.lower():
                continue
            if modell_cell and self._is_skip_row(modell_cell):
                continue

            # Need a model name
            if not modell_cell:
                continue

            # Parse count
            anzahl_raw = row[anzahl_col] if anzahl_col < len(row) else None
            anzahl = DataNormalizer.normalize_anzahl(anzahl_raw)
            if anzahl <= 0:
                continue

            # Split combined brand+model string
            marke_raw, modell_raw = self._split_brand_model(modell_cell)
            if not marke_raw:
                continue

            marke = DataNormalizer.normalize_marke(marke_raw)
            modell = DataNormalizer.normalize_modell(modell_raw) if modell_raw else None

            if not marke or not modell:
                continue

            results.append({
                'marke': marke,
                'modell': modell,
                'segment': current_segment,
                'jahr': year,
                'monat': month,
                'anzahl': anzahl,
            })

        logger.info(f"FZ11 {year}/{month:02d}: {len(results)} Datensaetze geparst")
        return results
