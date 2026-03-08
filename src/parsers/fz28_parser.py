"""
FZ28 Parser: Neuzulassungen mit alternativen Antrieben.
Monatliche Publikation mit Aufschluesselung nach BEV, PHEV, Hybrid, Gas, H2.

Relevante Sheets im monatlichen Format (Stand 2024):
- FZ 28.4: Marken-Aufschluesselung mit allen Antriebsarten
- FZ 28.8: Segment-Aufschluesselung
- FZ 28.9: Bundesland-Aufschluesselung

Spaltenstruktur FZ 28.4 (Zeile 7-11 = Header):
  Col 1:  Marke
  Col 2:  Anzahl insgesamt (alle Antriebe inkl. Verbrenner)
  Col 3:  Darunter alternative Antriebe - Anzahl
  Col 4:  Anteil in %
  Col 5:  Elektro-Antriebe Anzahl insgesamt
  Col 6:  Elektro Anteil in %
  Col 7:  davon Elektro (BEV)
  Col 8:  davon Brennstoffzelle (Wasserstoff/FCEV)
  Col 9:  davon Plug-in-Hybrid
  Col 10: Hybrid (ohne Plug-in) Anzahl insgesamt
  Col 11: darunter Voll-Hybrid
  Col 12: darunter Benzin-Hybrid
  Col 13: darunter Voll-Hybrid (Benzin)
  Col 14: darunter Diesel-Hybrid
  Col 15: darunter Voll-Hybrid (Diesel)
  Col 16: Gas insgesamt
  Col 17: Wasserstoff
"""
import logging
from pathlib import Path

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ28Parser(BaseParser):
    """Parser fuer FZ28: Monatliche Neuzulassungen mit alternativen Antrieben."""

    QUELLE_KUERZEL = 'FZ28'

    SKIP_PATTERNS = [
        'insgesamt', 'zusammen', 'gesamt', 'summe',
        'sonstige', 'übrige', 'andere',
        'quelle:', 'stand:', 'datum:',
        'kraftfahrt-bundesamt', 'zurück',
    ]

    # Fuel type column mappings for FZ 28.4 sheet.
    # Maps (column_index, fuel_type_name).
    # Column indices are 0-based relative to the data area (col B = index 1).
    FUEL_COLUMNS = {
        7:  'Elektro',           # BEV
        8:  'Brennstoffzelle',   # FCEV / Wasserstoff-Antrieb
        9:  'Plug-in-Hybrid',    # PHEV
        10: 'Hybrid',            # Hybrid (ohne Plug-in)
        16: 'Gas',               # Erdgas/CNG + LPG
    }

    def _is_skip_row(self, text):
        """Prueft ob eine Zeile uebersprungen werden soll."""
        if not text:
            return True
        lower = str(text).strip().lower()
        return any(pattern in lower for pattern in self.SKIP_PATTERNS)

    def parse(self, filepath):
        """
        Parst eine monatliche FZ28 Excel-Datei.

        Returns:
            list: Liste von Dicts mit keys:
                marke, modell, jahr, monat, anzahl, kraftstoff
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year:
            logger.error(f"Kann Jahr nicht bestimmen: {filepath}")
            self.close()
            return []

        monat = month if month else 0
        results = []
        sheet_names = self.get_sheet_names()

        # Marken-Sheet finden: Suche nach Sheet mit "Marke" in Spalte B
        # Sheet-Nummer variiert (FZ 28.4 ab 2024, FZ 28.5 in 2023, etc.)
        brand_sheet_name = self._find_brand_sheet(sheet_names)

        if brand_sheet_name:
            sheet = self.get_sheet(name=brand_sheet_name)
            if sheet:
                results = self._parse_brand_fuel_sheet(sheet, year, monat)
        else:
            logger.warning(f"FZ28 {year}/{monat}: Kein Marken-Sheet gefunden")

        self.close()
        logger.info(f"FZ28 {year}/{monat:02d}: {len(results)} Datensaetze geparst")
        return results

    def _find_brand_sheet(self, sheet_names):
        """
        Findet das Sheet mit Marken-Aufschluesselung.
        Prueft Sheets mit 'FZ 28.' im Namen auf 'Marke' in Spalte B.
        """
        for name in sheet_names:
            if '28.' not in name:
                continue
            sheet = self.get_sheet(name=name)
            if not sheet:
                continue
            rows = list(sheet.iter_rows(values_only=True, max_row=15))
            for row in rows:
                if row and len(row) > 1 and row[1]:
                    cell = str(row[1]).strip().lower()
                    if cell == 'marke':
                        logger.info(f"Marken-Sheet gefunden: {name}")
                        return name
        return None

    def _parse_brand_fuel_sheet(self, sheet, year, monat):
        """
        Parst FZ 28.4: Marken mit Antriebsart-Aufschluesselung.

        Jede Markenzeile wird in mehrere Records aufgespalten,
        einen pro Antriebsart mit Anzahl > 0.
        """
        results = []
        rows = list(sheet.iter_rows(values_only=True))

        # Finde die Headerzeile mit 'Marke'
        data_start = None
        for i, row in enumerate(rows):
            if not row:
                continue
            cell1 = str(row[1]).strip().lower() if len(row) > 1 and row[1] else ''
            if 'marke' in cell1:
                # Daten beginnen nach dem multi-row header (typisch 4-5 Zeilen)
                data_start = i + 5  # Skip header rows
                break

        if data_start is None:
            logger.warning("FZ 28.4: Konnte Headerzeile nicht finden")
            return results

        # Dynamisch die Spaltenindizes bestimmen
        fuel_cols = self._detect_fuel_columns(rows, data_start)

        for i in range(data_start, len(rows)):
            row = rows[i]
            if not row or len(row) < 3:
                continue

            marke_raw = str(row[1]).strip() if row[1] else ''
            if not marke_raw or self._is_skip_row(marke_raw):
                continue

            # Copyright/Fussnoten beenden
            if '©' in marke_raw or 'kraftfahrt' in marke_raw.lower():
                break

            norm_marke = DataNormalizer.normalize_marke(marke_raw)
            if not norm_marke:
                continue

            # Fuer jede Antriebsart einen Record erzeugen
            for col_idx, kraftstoff in fuel_cols.items():
                if col_idx >= len(row):
                    continue

                anzahl = self._parse_cell_value(row[col_idx])
                if anzahl > 0:
                    results.append({
                        'marke': norm_marke,
                        'modell': 'Gesamt',
                        'jahr': year,
                        'monat': monat,
                        'anzahl': anzahl,
                        'kraftstoff': kraftstoff,
                    })

            # Konventionelle Antriebe berechnen (Gesamt minus Alternative)
            total = self._parse_cell_value(row[2]) if len(row) > 2 else 0
            alt_total = self._parse_cell_value(row[3]) if len(row) > 3 else 0
            konventionell = total - alt_total
            if konventionell > 0:
                results.append({
                    'marke': norm_marke,
                    'modell': 'Gesamt',
                    'jahr': year,
                    'monat': monat,
                    'anzahl': konventionell,
                    'kraftstoff': 'Konventionell',
                })

        return results

    def _detect_fuel_columns(self, rows, data_start):
        """
        Versucht die Kraftstoff-Spalten dynamisch aus den Header-Zeilen zu erkennen.
        Faellt auf Standard-Mapping zurueck wenn nicht erfolgreich.
        """
        # Pruefe die Header-Zeilen (typisch 4-5 Zeilen vor data_start)
        for i in range(max(0, data_start - 6), data_start):
            if i >= len(rows) or not rows[i]:
                continue
            row_str = [str(c).strip().lower() if c else '' for c in rows[i]]

            detected = {}
            for j, cell in enumerate(row_str):
                if 'elektro (bev)' in cell or ('elektro' in cell and 'plug' not in cell and 'antrieb' not in cell and j > 5):
                    detected[j] = 'Elektro'
                elif 'brennstoffzelle' in cell:
                    detected[j] = 'Brennstoffzelle'
                elif 'plug-in' in cell or 'plug in' in cell:
                    detected[j] = 'Plug-in-Hybrid'
                elif 'hybrid' in cell and 'plug' not in cell and 'voll' not in cell and 'benzin' not in cell and 'diesel' not in cell:
                    if 'ohne' in cell or j > 8:
                        detected[j] = 'Hybrid'
                elif 'gas' in cell and 'insgesamt' in cell:
                    detected[j] = 'Gas'

            if len(detected) >= 3:
                logger.debug(f"Spalten dynamisch erkannt: {detected}")
                return detected

        # Fallback auf Standard-Mapping
        logger.debug("Verwende Standard-Spalten-Mapping")
        return self.FUEL_COLUMNS

    def _parse_cell_value(self, value):
        """Parst einen Zellenwert zu int, behandelt '-' und None."""
        if value is None or str(value).strip() in ('', '-', '.', '–'):
            return 0
        return DataNormalizer.normalize_anzahl(value)
