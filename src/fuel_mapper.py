"""
Fuel Mapper: Verknuepft FZ28 Kraftstoff-Verteilungen mit FZ10 Modell-Daten.

FZ28 liefert monatliche Kraftstoff-Aufschluesselungen pro Marke (Brand-Level).
FZ10 liefert monatliche Neuzulassungen pro Modell, aber OHNE Kraftstoff.

Dieser Mapper berechnet geschaetzte Kraftstoff-Verteilungen pro Modell,
indem er die FZ28-Markenverteilung proportional auf FZ10-Modelldaten anwendet.

Methodik:
  1. Fuer jede Marke+Monat: Hole FZ28 Kraftstoff-Anteile
  2. Fuer jedes Modell dieser Marke in FZ10: Verteile die Gesamtzahl proportional
  3. Speichere mit quelle_id=FZ10_EST und estimation_method='fz28_proportional'

Einschraenkung: Die monatliche Verteilung ist eine Schaetzung. Fuer rein elektrische
Modelle (z.B. Tesla) ist die Zuordnung exakt (100% BEV).
"""
import logging
from src.database import db

logger = logging.getLogger(__name__)


class FuelMapper:
    """Verknuepft FZ28 Kraftstoff-Daten mit FZ10 Modell-Daten."""

    ESTIMATION_METHOD = 'fz28_proportional'

    def __init__(self):
        self._land_id = None
        self._fz10_quelle_id = None
        self._fz28_quelle_id = None
        self._est_quelle_id = None

    def _init_ids(self):
        """Laedt die benoetigten IDs aus der Datenbank."""
        self._land_id = db.get_land_id('DE')
        self._fz10_quelle_id = db.get_quelle_id('FZ10', 'DE')
        self._fz28_quelle_id = db.get_quelle_id('FZ28', 'DE')
        self._est_quelle_id = db.get_quelle_id('FZ10_EST', 'DE')

        if not all([self._land_id, self._fz10_quelle_id, self._fz28_quelle_id, self._est_quelle_id]):
            raise RuntimeError(
                "Fehlende IDs: land={}, FZ10={}, FZ28={}, FZ10_EST={}".format(
                    self._land_id, self._fz10_quelle_id, self._fz28_quelle_id, self._est_quelle_id
                )
            )

    def run(self, year=None, month=None):
        """
        Fuehrt das Kraftstoff-Mapping durch.

        Args:
            year: Optionales Jahr (None = alle verfuegbaren)
            month: Optionaler Monat (None = alle Monate)

        Returns:
            dict: Statistiken {mapped, skipped, errors}
        """
        self._init_ids()
        stats = {'mapped': 0, 'skipped_no_fz28': 0, 'skipped_no_fz10': 0, 'errors': 0}

        # Hole alle Marke+Monat-Kombinationen aus FZ28
        periods = self._get_fz28_periods(year, month)
        logger.info(f"Kraftstoff-Mapping: {len(periods)} Marke+Monat-Kombinationen gefunden")

        for period in periods:
            marke_id = period['marke_id']
            marke_name = period['marke_name']
            jahr = period['jahr']
            monat = period['monat']

            try:
                mapped = self._map_brand_month(marke_id, marke_name, jahr, monat)
                stats['mapped'] += mapped
            except Exception as e:
                logger.error(f"Fehler bei {marke_name} {jahr}/{monat}: {e}")
                stats['errors'] += 1

        logger.info(
            f"Kraftstoff-Mapping abgeschlossen: {stats['mapped']} Datensaetze geschrieben, "
            f"{stats['errors']} Fehler"
        )
        return stats

    def _get_fz28_periods(self, year=None, month=None):
        """Gibt alle Marke+Jahr+Monat-Kombinationen aus FZ28 zurueck."""
        query = """
            SELECT DISTINCT m2.id as marke_id, m2.name as marke_name, n.jahr, n.monat
            FROM neuzulassungen n
            JOIN modelle m ON n.modell_id = m.id
            JOIN marken m2 ON m.marke_id = m2.id
            WHERE n.quelle_id = %s AND n.monat > 0
        """
        params = [self._fz28_quelle_id]

        if year:
            query += " AND n.jahr = %s"
            params.append(year)
        if month:
            query += " AND n.monat = %s"
            params.append(month)

        query += " ORDER BY n.jahr, n.monat, m2.name"
        return db.execute(query, params)

    def _map_brand_month(self, marke_id, marke_name, jahr, monat):
        """
        Mappt Kraftstoff-Verteilung einer Marke fuer einen Monat auf alle ihre Modelle.

        Returns:
            int: Anzahl geschriebener Datensaetze
        """
        # 1. FZ28 Kraftstoff-Verteilung holen (Marke-Level)
        fuel_dist = self._get_fuel_distribution(marke_id, jahr, monat)
        if not fuel_dist:
            return 0

        # 2. FZ10 Modelle dieser Marke fuer diesen Monat holen
        models = self._get_fz10_models(marke_id, jahr, monat)
        if not models:
            return 0

        # 3. Fuer jedes Modell die Kraftstoff-Verteilung anwenden
        written = 0
        for model in models:
            modell_id = model['modell_id']
            fz10_total = model['anzahl']

            for kraftstoff, anteil in fuel_dist.items():
                estimated = round(fz10_total * anteil)
                if estimated <= 0:
                    continue

                db.insert_or_update(
                    """INSERT INTO neuzulassungen
                       (land_id, modell_id, jahr, monat, anzahl, kraftstoff, quelle_id, estimation_method)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                           anzahl = VALUES(anzahl),
                           estimation_method = VALUES(estimation_method)""",
                    (self._land_id, modell_id, jahr, monat, estimated,
                     kraftstoff, self._est_quelle_id, self.ESTIMATION_METHOD)
                )
                written += 1

        return written

    def _get_fuel_distribution(self, marke_id, jahr, monat):
        """
        Holt die prozentuale Kraftstoff-Verteilung einer Marke aus FZ28.

        Returns:
            dict: {kraftstoff_name: anteil} (Anteile summieren sich auf ~1.0)
                  oder None wenn keine Daten
        """
        # FZ28 speichert Marken-Gesamt als modell "Gesamt"
        rows = db.execute(
            """SELECT kraftstoff, anzahl
               FROM neuzulassungen n
               JOIN modelle m ON n.modell_id = m.id
               WHERE m.marke_id = %s AND n.quelle_id = %s
                     AND n.jahr = %s AND n.monat = %s
                     AND n.kraftstoff IS NOT NULL""",
            (marke_id, self._fz28_quelle_id, jahr, monat)
        )

        if not rows:
            return None

        total = sum(r['anzahl'] for r in rows)
        if total <= 0:
            return None

        return {r['kraftstoff']: r['anzahl'] / total for r in rows}

    def _get_fz10_models(self, marke_id, jahr, monat):
        """
        Holt alle FZ10-Modelle einer Marke fuer einen Monat.
        Filtert das 'Gesamt'-Modell heraus.

        Returns:
            list: [{'modell_id': int, 'anzahl': int}, ...]
        """
        return db.execute(
            """SELECT n.modell_id, n.anzahl
               FROM neuzulassungen n
               JOIN modelle m ON n.modell_id = m.id
               WHERE m.marke_id = %s AND n.quelle_id = %s
                     AND n.jahr = %s AND n.monat = %s
                     AND n.kraftstoff IS NULL
                     AND m.name != 'Gesamt'""",
            (marke_id, self._fz10_quelle_id, jahr, monat)
        )
