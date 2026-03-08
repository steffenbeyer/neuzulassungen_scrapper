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
        """Findet die Zeile mit Spaltenueberschriften."""
        for i, row in enumerate(rows):
            if not row:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in row]
            # Typische Header-Begriffe
            header_hints = ['fahrzeugart', 'bundesland', 'kraftstoff', 'anzahl', 'monat', 'januar', 'februar']
            if any(any(h in cell for h in header_hints) for cell in row_str if cell):
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
        label_col = 0
        anzahl_col = None
        header_str = [str(c).lower() if c else '' for c in header_row]

        for j, cell in enumerate(header_str):
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

        if anzahl_col is None:
            # Fallback: erste numerische Spalte nach label_col
            for j in range(1, min(15, len(header_row))):
                if j < len(rows[header_idx + 1] if header_idx + 1 < len(rows) else []):
                    val = rows[header_idx + 1][j] if header_idx + 1 < len(rows) else None
                    if val is not None:
                        try:
                            int(DataNormalizer.normalize_anzahl(val))
                            anzahl_col = j
                            break
                        except (ValueError, TypeError):
                            pass

        if anzahl_col is None:
            anzahl_col = 1

        for i in range(header_idx + 1, len(rows)):
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
