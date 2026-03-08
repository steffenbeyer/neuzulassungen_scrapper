"""
FZ28 Parser: Neuzulassungen mit alternativen Antrieben.
Elektro, Hybrid, Plug-in-Hybrid, Wasserstoff, Erdgas etc.
"""
import logging
from pathlib import Path

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ28Parser(BaseParser):
    """Parser fuer FZ28: Neuzulassungen mit alternativen Antrieben."""

    QUELLE_KUERZEL = 'FZ28'

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

    def _is_marke_row(self, row_values):
        """Erkennt ob eine Zeile eine Marken-Ueberschrift ist."""
        first_col = str(row_values[0]).strip() if row_values[0] else ''
        second_col = str(row_values[1]).strip() if len(row_values) > 1 and row_values[1] else ''

        if not first_col:
            return False, None

        if not second_col and first_col and first_col == first_col.upper():
            if len(first_col) >= 2 and not self._is_skip_row(first_col):
                return True, first_col

        return False, None

    def _detect_kraftstoff_from_sheet(self, sheet_name):
        """Erkennt den Kraftstofftyp aus dem Sheet-Namen."""
        name_lower = (sheet_name or '').lower()

        mapping = {
            'elektro': 'Elektro',
            'bev': 'Elektro',
            'batterie': 'Elektro',
            'plug-in': 'Plug-in-Hybrid',
            'phev': 'Plug-in-Hybrid',
            'hybrid': 'Hybrid',
            'wasserstoff': 'Wasserstoff',
            'h2': 'Wasserstoff',
            'erdgas': 'Erdgas/CNG',
            'cng': 'Erdgas/CNG',
            'lpg': 'Autogas/LPG',
            'autogas': 'Autogas/LPG',
        }

        for key, value in mapping.items():
            if key in name_lower:
                return value

        return None

    def parse(self, filepath):
        """
        Parst eine FZ28 Excel-Datei (alternative Antriebe).

        Returns:
            list: Liste von Dicts mit keys:
                marke, modell, jahr, monat (=0), anzahl, kraftstoff
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year:
            logger.error(f"Kann Jahr nicht bestimmen: {filepath}")
            self.close()
            return []

        # FZ28 ist monatlich verfuegbar (frueherer Code ging von jaehrlich aus)
        monat = month if month else 0
        results = []
        sheet_names = self.get_sheet_names()

        logger.info(f"FZ28 {year}: {len(sheet_names)} Sheets: {sheet_names}")

        for sheet_name in sheet_names:
            sheet = self.get_sheet(name=sheet_name)
            if not sheet:
                continue

            kraftstoff = self._detect_kraftstoff_from_sheet(sheet_name)
            sheet_results = self._parse_sheet(sheet, sheet_name, year, monat, kraftstoff)
            results.extend(sheet_results)

        self.close()

        logger.info(f"FZ28 {year}: {len(results)} Datensaetze geparst")
        return results

    def _parse_sheet(self, sheet, sheet_name, year, monat, kraftstoff=None):
        """Parst ein einzelnes Sheet der FZ28-Datei."""
        results = []
        current_marke = None
        rows = list(sheet.iter_rows(values_only=True))

        header_row = None
        modell_col = 0
        anzahl_col = None
        kraftstoff_col = None

        # Header-Zeile finden
        for i, row in enumerate(rows):
            if not row:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]

            for j, cell in enumerate(row_str):
                if any(h in cell for h in ['modellreihe', 'modell', 'handelsname']):
                    modell_col = j
                    header_row = i
                elif any(h in cell for h in ['marke', 'hersteller']) and header_row is None:
                    modell_col = j
                elif any(h in cell for h in ['kraftstoff', 'antrieb']):
                    kraftstoff_col = j

            if header_row == i:
                for j, cell in enumerate(row_str):
                    if cell and any(m in cell for m in [
                        'anzahl', 'neuzulassungen', 'zulassungen'
                    ]):
                        anzahl_col = j
                        break

                if anzahl_col is None and modell_col is not None:
                    for j in range(modell_col + 1, min(modell_col + 5, len(row))):
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
            anzahl_col = modell_col + 1

        # Daten parsen
        for i in range(header_row + 1, len(rows)):
            row = rows[i]
            if not row or len(row) <= max(modell_col, anzahl_col):
                continue

            is_marke, marke_name = self._is_marke_row(row)
            if is_marke:
                current_marke = marke_name
                marke_anzahl = DataNormalizer.normalize_anzahl(
                    row[anzahl_col] if anzahl_col < len(row) else None
                )
                row_kraftstoff = None
                if kraftstoff_col is not None and kraftstoff_col < len(row) and row[kraftstoff_col]:
                    row_kraftstoff = DataNormalizer.normalize_kraftstoff(
                        str(row[kraftstoff_col]).strip()
                    )
                final_kraftstoff = row_kraftstoff or kraftstoff

                if marke_anzahl > 0 and current_marke and final_kraftstoff:
                    norm_marke = DataNormalizer.normalize_marke(current_marke)
                    if norm_marke:
                        results.append({
                            'marke': norm_marke,
                            'modell': DataNormalizer.normalize_modell('Gesamt') or 'Gesamt',
                            'jahr': year,
                            'monat': monat,
                            'anzahl': marke_anzahl,
                            'kraftstoff': final_kraftstoff,
                        })
                continue

            if current_marke:
                modell_name = str(row[modell_col]).strip() if row[modell_col] else None
                norm_modell = DataNormalizer.normalize_modell(modell_name) if modell_name else None

                if modell_name and not self._is_skip_row(modell_name) and norm_modell:
                    anzahl = DataNormalizer.normalize_anzahl(
                        row[anzahl_col] if anzahl_col < len(row) else None
                    )
                    row_kraftstoff = None
                    if kraftstoff_col is not None and kraftstoff_col < len(row) and row[kraftstoff_col]:
                        row_kraftstoff = DataNormalizer.normalize_kraftstoff(
                            str(row[kraftstoff_col]).strip()
                        )
                    final_kraftstoff = row_kraftstoff or kraftstoff

                    if anzahl > 0 and final_kraftstoff:
                        norm_marke = DataNormalizer.normalize_marke(current_marke)
                        if norm_marke:
                            results.append({
                                'marke': norm_marke,
                                'modell': norm_modell,
                                'jahr': year,
                                'monat': monat,
                                'anzahl': anzahl,
                                'kraftstoff': final_kraftstoff,
                            })

        return results
