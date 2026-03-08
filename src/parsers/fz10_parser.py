"""
FZ10 Parser: Neuzulassungen von PKW nach Marken und Modellreihen.
Dies ist der KERN-Datensatz fuer AlleZulassungen.

Tatsaechliche Struktur der FZ10-Excel-Datei (Stand 2025):
- Sheets: Deckblatt, Impressum, Inhaltsverzeichnis, FZ 10.1
- Sheet "FZ 10.1" enthaelt die Daten:
  - Zeile 8 (0-indexed: 7): Header "Marke | Modellreihe | Januar 2025 | Jan.-2025 | Anteil %"
  - Spalte A (Index 0): leer
  - Spalte B (Index 1): Marke (nur in erster Zeile einer Gruppe, dann leer)
  - Spalte C (Index 2): Modellreihe
  - Spalte D (Index 3): Neuzulassungen im Monat
  - Spalte E (Index 4): Kumuliert (bei Januar = gleich wie Monat)
  - Spalte F (Index 5): Anteil in %
  - "MARKE ZUSAMMEN"-Zeilen: Summe pro Marke
"""
import logging
from pathlib import Path

from src.parsers.base_parser import BaseParser
from src.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class FZ10Parser(BaseParser):
    """Parser fuer FZ10: PKW Neuzulassungen nach Marken und Modellreihen."""

    QUELLE_KUERZEL = 'FZ10'

    # Zeilen/Begriffe die keine echten Modelle sind
    SKIP_WORDS = [
        'insgesamt', 'zusammen', 'sonstige', 'übrige',
        'personenkraftwagen', 'kraftfahrt-bundesamt',
        'neuzulassungen', 'fahrzeugzulassungen',
        'marke', 'modellreihe', 'anteil',
        'zurück', 'inhaltsverzeichnis',
    ]

    def parse(self, filepath):
        """
        Parst eine FZ10 Excel-Datei.

        Returns:
            list: Liste von Dicts mit keys:
                - marke, modell, jahr, monat, anzahl, kraftstoff
        """
        self.load(filepath)
        year, month = self.extract_year_month_from_filename()

        if not year or not month:
            logger.error(f"Kann Jahr/Monat nicht bestimmen: {filepath}")
            self.close()
            return []

        results = []

        # Das Daten-Sheet finden (typischerweise "FZ 10.1" oder das letzte Sheet)
        data_sheet = None
        for name in self.get_sheet_names():
            if 'FZ' in name.upper() and '10' in name:
                data_sheet = self.get_sheet(name=name)
                logger.info(f"Verwende Sheet: {name}")
                break

        if not data_sheet:
            # Fallback: Letztes Sheet nehmen
            sheets = self.get_sheet_names()
            if sheets:
                data_sheet = self.get_sheet(name=sheets[-1])
                logger.info(f"Fallback Sheet: {sheets[-1]}")

        if not data_sheet:
            logger.error("Kein Daten-Sheet gefunden!")
            self.close()
            return []

        rows = list(data_sheet.iter_rows(values_only=True))
        results = self._parse_data_rows(rows, year, month)

        self.close()
        logger.info(f"FZ10 {year}/{month:02d}: {len(results)} Datensaetze geparst")
        return results

    def _parse_data_rows(self, rows, year, month):
        """Parst die Datenzeilen des FZ 10.1 Sheets."""
        results = []
        current_marke = None
        header_found = False
        marke_col = None
        modell_col = None
        anzahl_col = None

        for i, row in enumerate(rows):
            if not row:
                continue

            # In Strings konvertieren fuer einfacheres Pruefen
            cells = [str(c).strip() if c is not None else '' for c in row]

            # Header-Zeile erkennen: Enthaelt "Marke" und "Modellreihe"
            if not header_found:
                cells_lower = [c.lower() for c in cells]
                for j, cell in enumerate(cells_lower):
                    if 'marke' in cell and 'modell' not in cell:
                        marke_col = j
                    elif 'modellreihe' in cell or 'modell' in cell:
                        modell_col = j

                if marke_col is not None and modell_col is not None:
                    header_found = True
                    # Anzahl-Spalte: Die naechste Spalte nach Modell
                    anzahl_col = modell_col + 1
                    logger.debug(f"Header in Zeile {i}: Marke={marke_col}, Modell={modell_col}, Anzahl={anzahl_col}")
                continue

            if not header_found:
                continue

            # Sicherstellen dass genug Spalten vorhanden
            if len(row) <= max(marke_col, modell_col, anzahl_col):
                continue

            marke_val = cells[marke_col] if marke_col < len(cells) else ''
            modell_val = cells[modell_col] if modell_col < len(cells) else ''
            anzahl_raw = row[anzahl_col] if anzahl_col < len(row) else None

            # Leere Zeilen ueberspringen
            if not marke_val and not modell_val:
                continue

            # Skip-Zeilen pruefen
            combined = (marke_val + ' ' + modell_val).lower()
            if any(skip in combined for skip in self.SKIP_WORDS):
                # "ZUSAMMEN"-Zeilen ueberspringen (Marken-Summen)
                if 'zusammen' in combined:
                    continue
                # Andere Skip-Woerter auch ueberspringen
                if not marke_val or any(skip in marke_val.lower() for skip in self.SKIP_WORDS):
                    continue

            # Neue Marke erkennen
            if marke_val:
                # Pruefen ob "ZUSAMMEN" in der Marke steht
                if 'zusammen' in marke_val.lower():
                    continue
                current_marke = marke_val

            if not current_marke:
                continue

            # Modell extrahieren
            if modell_val:
                modell_name = modell_val
            else:
                # Wenn kein Modell angegeben, koennte es die Marken-Zeile selbst sein
                # mit einer Gesamtzahl
                continue

            # "Sonstige" Modelle behalten (koennen relevant sein)
            # Aber "ZUSAMMEN" nicht
            if 'zusammen' in modell_name.lower():
                continue

            # Anzahl extrahieren
            anzahl = DataNormalizer.normalize_anzahl(anzahl_raw)
            if anzahl <= 0:
                continue

            results.append({
                'marke': current_marke,
                'modell': modell_name,
                'jahr': year,
                'monat': month,
                'anzahl': anzahl,
                'kraftstoff': None,  # FZ10.1 hat keine Kraftstoff-Aufschluesselung
            })

        return results
