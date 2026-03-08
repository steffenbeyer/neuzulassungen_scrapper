"""
FZ8 Parser: Monatliche Kraftfahrzeugzulassungen nach ausgewaehlten Merkmalen.

Die FZ8-Excel-Datei enthaelt:
- Fahrzeugzulassungen nach Fahrzeugart, Bundesland, Kraftstoff
- Mehrere Sheets fuer unterschiedliche Aufbereitungen
- Aggregierte Daten pro Kategorie (nicht pro Modell)
"""
import logging

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ8Parser(BaseParser):
    """Parser fuer FZ8: Monatliche Zulassungen nach Fahrzeugart, Bundesland, Kraftstoff."""

    QUELLE_KUERZEL = 'FZ8'

    SKIP_PATTERNS = [
        'insgesamt', 'zusammen', 'gesamt', 'summe',
        'sonstige', 'übrige', 'andere',
        'quelle:', 'stand:', 'datum:',
    ]

    # Bekannte Fahrzeugarten
    FAHRZEUGART_KEYWORDS = [
        'personenkraftwagen', 'pkw', 'lastkraftwagen', 'lkw',
        'krafträder', 'motorräder', 'kraftrad', 'motorrad',
        'busse', 'wohnmobile', 'anhänger', 'gesamt',
    ]

    # Bundeslaender
    BUNDESLAND_KEYWORDS = [
        'baden-württemberg', 'bayern', 'berlin', 'brandenburg', 'bremen',
        'hamburg', 'hessen', 'mecklenburg-vorpommern', 'niedersachsen',
        'nordrhein-westfalen', 'rheinland-pfalz', 'saarland', 'sachsen',
        'sachsen-anhalt', 'schleswig-holstein', 'thüringen', 'deutschland',
    ]

    def _is_skip_row(self, text):
        """Prueft ob eine Zeile uebersprungen werden soll."""
        if not text:
            return True
        lower = str(text).strip().lower()
        return any(pattern in lower for pattern in self.SKIP_PATTERNS)

    def _detect_breakdown_type(self, sheet_name, header_row):
        """
        Erkennt den Aufbereitungstyp eines Sheets.
        Returns: 'fahrzeugart', 'bundesland', 'kraftstoff' oder None
        """
        combined = (sheet_name or '').lower()
        if header_row:
            combined += ' ' + ' '.join(str(c).lower() for c in header_row if c)

        if any(kw in combined for kw in ['fahrzeugart', 'fz-art', 'art der fahrzeuge']):
            return 'fahrzeugart'
        if any(kw in combined for kw in ['bundesland', 'land', 'region', 'kreis']):
            return 'bundesland'
        if any(kw in combined for kw in ['kraftstoff', 'antrieb', 'energieträger']):
            return 'kraftstoff'

        return None

    def _find_header_row(self, rows):
        """Findet die Zeile mit Spaltenueberschriften.

        Die Header-Zeile hat typischerweise mehrere nicht-leere Zellen mit
        Begriffen wie 'Marke', 'Anzahl', Monatsnamen etc.
        Wichtig: Nicht mit Titelzeilen verwechseln, die nur eine Zelle haben.
        """
        label_hints = ['marke', 'fahrzeugart', 'bundesland', 'kraftstoff',
                       'antriebsart', 'segment', 'land', 'merkmal']
        month_hints = ['januar', 'februar', 'märz', 'maerz', 'april', 'mai', 'juni',
                       'juli', 'august', 'september', 'oktober', 'november', 'dezember']

        for i, row in enumerate(rows):
            if not row:
                continue
            # Nur Zeilen mit mindestens 2 nicht-leeren Zellen beruecksichtigen
            non_empty = [str(c).strip().lower() for c in row if c is not None and str(c).strip()]
            if len(non_empty) < 2:
                continue

            row_str = [str(c).strip().lower() if c else '' for c in row]
            has_label = any(any(h in cell for h in label_hints) for cell in row_str)
            has_value = any(any(h in cell for h in month_hints + ['anzahl', 'neuzulassungen'])
                           for cell in row_str)
            if has_label and has_value:
                return i, row

        # Fallback: Zeile mit Label-Hint und Sub-Header mit 'Anzahl'
        for i, row in enumerate(rows):
            if not row:
                continue
            non_empty = [str(c).strip().lower() for c in row if c is not None and str(c).strip()]
            if len(non_empty) < 2:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]
            if any(any(h in cell for h in label_hints) for cell in row_str):
                if i + 1 < len(rows) and rows[i + 1]:
                    next_str = [str(c).strip().lower() if c else '' for c in rows[i + 1]]
                    if any('anzahl' in c for c in next_str):
                        return i, row

        return 0, rows[0] if rows else []

    def parse(self, filepath):
        """
        Parst eine FZ8 Excel-Datei.

        Returns:
            list: Liste von Dicts mit keys:
                jahr, monat, fahrzeugart, region, kraftstoff, anzahl
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year:
            logger.error(f"Kann Jahr nicht bestimmen: {filepath}")
            self.close()
            return []

        results = []
        sheet_names = self.get_sheet_names()

        logger.info(f"FZ8 {year}/{month or '?'}: {len(sheet_names)} Sheets: {sheet_names}")

        for sheet_name in sheet_names:
            sheet = self.get_sheet(name=sheet_name)
            if not sheet:
                continue

            sheet_results = self._parse_sheet(sheet, sheet_name, year, month)
            results.extend(sheet_results)

        self.close()

        logger.info(f"FZ8 {year}/{month or '?'}: {len(results)} Datensaetze geparst")
        return results

    def _parse_sheet(self, sheet, sheet_name, year, month):
        """Parst ein einzelnes Sheet der FZ8-Datei."""
        results = []
        rows = list(sheet.iter_rows(values_only=True))

        if not rows:
            return results

        header_idx, header_row = self._find_header_row(rows)
        breakdown_type = self._detect_breakdown_type(sheet_name, header_row)

        # Spaltenindizes ermitteln
        label_col = None
        anzahl_col = None
        header_str = [str(c).lower() if c else '' for c in header_row]

        for j, cell in enumerate(header_str):
            if label_col is None and cell and any(h in cell for h in [
                'marke', 'fahrzeugart', 'bundesland', 'kraftstoff', 'land',
                'antriebsart', 'segment', 'merkmal'
            ]):
                label_col = j
            if any(h in cell for h in ['anzahl', 'neuzulassungen', 'zulassungen']):
                anzahl_col = j
                break
            if any(m in cell for m in [
                'januar', 'februar', 'märz', 'maerz', 'april', 'mai', 'juni',
                'juli', 'august', 'september', 'oktober', 'november', 'dezember'
            ]):
                if month and str(month) in cell:
                    anzahl_col = j
                    break
                anzahl_col = j  # Fallback: erste Monatsspalte

        if label_col is None:
            label_col = 1  # Default: Spalte B

        if anzahl_col is None:
            # Check sub-header row for 'Anzahl'
            if header_idx + 1 < len(rows) and rows[header_idx + 1]:
                sub_str = [str(c).lower() if c else '' for c in rows[header_idx + 1]]
                for j, cell in enumerate(sub_str):
                    if 'anzahl' in cell:
                        anzahl_col = j
                        break

        # Data starts after header + sub-header rows
        data_start = header_idx + 1
        if anzahl_col is not None and header_idx + 1 < len(rows) and rows[header_idx + 1]:
            sub_str = [str(c).lower() if c else '' for c in rows[header_idx + 1]]
            if any('anzahl' in c or 'anteil' in c for c in sub_str):
                data_start = header_idx + 2

        if anzahl_col is None:
            # Fallback: first column after label with numeric data
            test_row_idx = data_start
            if test_row_idx < len(rows) and rows[test_row_idx]:
                for j in range(label_col + 1, min(15, len(rows[test_row_idx]))):
                    val = rows[test_row_idx][j]
                    if val is not None:
                        try:
                            int(DataNormalizer.normalize_anzahl(val))
                            anzahl_col = j
                            break
                        except (ValueError, TypeError):
                            pass

        if anzahl_col is None:
            anzahl_col = label_col + 1

        for i in range(data_start, len(rows)):
            row = rows[i]
            if not row or len(row) <= max(label_col, anzahl_col):
                continue

            label = str(row[label_col]).strip() if row[label_col] else ''
            if not label or self._is_skip_row(label):
                continue

            anzahl = DataNormalizer.normalize_anzahl(row[anzahl_col] if anzahl_col < len(row) else None)
            if anzahl <= 0:
                continue

            record = {
                'jahr': year,
                'monat': month,
                'fahrzeugart': None,
                'region': None,
                'kraftstoff': None,
                'anzahl': anzahl,
            }

            label_lower = label.lower()
            norm_kraftstoff = DataNormalizer.normalize_kraftstoff(label)

            if breakdown_type == 'fahrzeugart':
                record['fahrzeugart'] = label if not self._is_skip_row(label) else None
            elif breakdown_type == 'bundesland':
                record['region'] = label
            elif breakdown_type == 'kraftstoff':
                record['kraftstoff'] = norm_kraftstoff
            else:
                # Unbekannter Typ: versuche aus Label zu erraten
                if any(kw in label_lower for kw in ['pkw', 'lkw', 'bus', 'kraftrad', 'wohnmobil']):
                    record['fahrzeugart'] = label
                elif any(kw in label_lower for kw in self.BUNDESLAND_KEYWORDS) or len(label) > 10:
                    record['region'] = label
                elif norm_kraftstoff and norm_kraftstoff != label:
                    record['kraftstoff'] = norm_kraftstoff
                else:
                    record['fahrzeugart'] = label

            results.append(record)

        return results
