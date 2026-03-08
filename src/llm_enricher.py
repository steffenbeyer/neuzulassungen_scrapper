"""
LLM-Enricher: Reichert Marken- und Modell-Daten mit OpenAI GPT an.

Vorteile gegenueber reiner Wikidata-Suche:
- Korrekte Disambiguierung (Tesla = Automarke, nicht SI-Einheit)
- SEO-freundliche, massgeschneiderte Beschreibungstexte auf Deutsch
- Zuverlaessige Fakten (Gruendungsjahr, Land, Website)
- Liefert den korrekten Wikidata-QID fuer Logo-/Bild-Download

Bilder werden weiterhin von Wikimedia Commons heruntergeladen
(LLMs koennen keine Bilder generieren), aber das LLM liefert den
korrekten Wikidata-QID damit die richtige Entitaet gefunden wird.

Usage:
    python main.py --mode llm-enrich-marken        # Marken anreichern
    python main.py --mode llm-enrich-modelle       # Modelle anreichern
    python main.py --mode llm-enrich-all           # Beides
    python main.py --mode llm-enrich-all --force   # Alles neu generieren
"""
import json
import logging
import os
import time

from config import Config
from src.database import db
from src.wikidata_importer import WikidataImporter

logger = logging.getLogger(__name__)

# ====================================================================
#  SYSTEM-PROMPTS
# ====================================================================

SYSTEM_PROMPT_MARKE = """Du bist ein Automobil-Experte und Content-Ersteller fuer allezulassungen.de,
eine deutsche Webseite ueber KFZ-Neuzulassungen und Fahrzeugstatistiken.

WICHTIG - KONTEXT: Alle Marken beziehen sich IMMER auf AUTOMOBILE/AUTOMARKEN:
- "Tesla" = Tesla, Inc. (Elektroautohersteller), NICHT die SI-Einheit
- "Smart" = smart automobile GmbH (Kleinwagen/E-Autos), NICHT das Adjektiv
- "Genesis" = Genesis Motor (Hyundai-Luxusmarke), NICHT das Buch der Bibel
- "Mini" = MINI (BMW-Tochtermarke), NICHT das Adjektiv "klein"
- "Alpine" = Alpine (Renault-Sportwagenmarke), NICHT der Alpenverein
- "DS" = DS Automobiles (Stellantis/Citroen-Premiummarke)
- "Ora" = ORA (Great Wall Motors E-Auto-Marke)
- "NIO" = NIO Inc. (chinesischer E-Autohersteller)
- "Lucid" = Lucid Motors (amerikanischer E-Autohersteller)
- "Cupra" = CUPRA (Seat-Sportmarke, Stellantis)
- "Man" = MAN Truck & Bus (Nutzfahrzeughersteller)
- "Ineos" = INEOS Automotive (Grenadier Gelaendewagen)

Antworte IMMER mit validem JSON in exakt diesem Format:
{
  "beschreibung": "2-3 informative Saetze auf Deutsch ueber die Automarke. Erwaehne wichtige Modelle, Positionierung und Besonderheiten. Geeignet fuer eine Webseite ueber Neuzulassungen.",
  "gruendungsjahr": 1937,
  "herkunftsland": "DE",
  "website": "https://www.volkswagen.de",
  "wikidata_id": "Q246"
}

Regeln:
- "herkunftsland": ISO 3166-1 Alpha-2 Code (DE, US, JP, KR, CN, FR, IT, GB, SE, RO, etc.)
- "gruendungsjahr": Gruendungsjahr als Automarke (nicht Mutterkonzern)
- "website": Offizielle deutschsprachige Website bevorzugen, sonst internationale
- "wikidata_id": Wikidata QID fuer die AUTOMARKE (nicht Konzern, nicht Person, nicht anderes)
  - Tesla Inc. = Q478214, NICHT Q163343 (Nikola Tesla)
  - smart automobile = Q156832
  - BMW = Q26678 (Automarke) 
- "beschreibung": KEIN Markdown, nur Fliesstext auf Deutsch
- Wenn du dir bei einem Wert unsicher bist, setze null"""

SYSTEM_PROMPT_MODELL = """Du bist ein Automobil-Experte und Content-Ersteller fuer allezulassungen.de,
eine deutsche Webseite ueber KFZ-Neuzulassungen und Fahrzeugstatistiken.

Antworte IMMER mit validem JSON in exakt diesem Format:
{
  "beschreibung": "2-3 informative Saetze auf Deutsch ueber dieses Automodell. Erwaehne Besonderheiten, Positionierung im Segment und ggf. Generationen.",
  "bauzeit_von": 2019,
  "bauzeit_bis": null,
  "fahrzeugklasse": "Kompaktklasse",
  "wikidata_id": "Q12345"
}

Regeln:
- "bauzeit_von": Erstes Produktionsjahr (der aktuellen oder letzten Generation)
- "bauzeit_bis": Letztes Produktionsjahr, null wenn noch in Produktion
- "fahrzeugklasse": Eine von: Kleinstwagen, Kleinwagen, Kompaktklasse, Mittelklasse, 
  Obere Mittelklasse, Oberklasse, Sportwagen, SUV, Gelaendewagen, Van, Transporter,
  Roadster, Coupe, Cabrio, Pickup, Nutzfahrzeug, Elektro-Kleinwagen
- "wikidata_id": Wikidata QID fuer dieses AUTO-MODELL (nicht die Marke!)
- "beschreibung": KEIN Markdown, nur Fliesstext auf Deutsch
- Wenn du dir bei einem Wert unsicher bist, setze null"""


class LLMEnricher:
    """Reichert Marken- und Modell-Daten mit OpenAI GPT an."""

    # Pause zwischen API-Calls (Rate Limiting)
    REQUEST_DELAY = 0.5

    def __init__(self):
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError(
                "OPENAI_API_KEY nicht gesetzt! "
                "Bitte in .env oder als Umgebungsvariable setzen."
            )

        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')

        # Wikidata-Importer fuer Logo-/Bild-Downloads wiederverwenden
        self.wiki_importer = WikidataImporter()

        logger.info(f"LLM-Enricher initialisiert (Modell: {self.model})")

    # ====================================================================
    #  OEFFENTLICHE METHODEN
    # ====================================================================

    def enrich_marken(self, force=False):
        """
        Reichert alle Marken mit LLM-generierten Daten an.

        Args:
            force: Wenn True, werden auch bereits angereicherte Marken ueberschrieben.
        """
        where = "" if force else "WHERE beschreibung IS NULL OR wikidata_id IS NULL"
        marken = db.execute(
            f"SELECT id, name, slug, wikidata_id FROM marken {where} ORDER BY name"
        )

        total = len(marken)
        stats = {'enriched': 0, 'errors': 0}

        logger.info("=" * 60)
        logger.info(f"LLM MARKEN-ANREICHERUNG ({total} Marken, Modell: {self.model})")
        logger.info("=" * 60)

        for i, marke in enumerate(marken, 1):
            name = marke['name']
            logger.info(f"[{i}/{total}] {name}")

            try:
                success = self._enrich_single_marke(marke)
                if success:
                    stats['enriched'] += 1
                    logger.info(f"  -> Erfolgreich angereichert")
                else:
                    stats['errors'] += 1
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  -> Fehler: {e}", exc_info=True)

            time.sleep(self.REQUEST_DELAY)

        logger.info("=" * 60)
        logger.info(
            f"ERGEBNIS: {stats['enriched']} angereichert, "
            f"{stats['errors']} Fehler"
        )
        logger.info("=" * 60)
        return stats

    def enrich_modelle(self, force=False):
        """
        Reichert alle Modelle mit LLM-generierten Daten an.

        Args:
            force: Wenn True, werden auch bereits angereicherte Modelle ueberschrieben.
        """
        where = "WHERE m.beschreibung IS NULL OR m.wikidata_id IS NULL" if not force else ""
        modelle = db.execute(f"""
            SELECT m.id, m.name, m.slug, m.wikidata_id,
                   mk.name AS marke_name, mk.slug AS marke_slug
            FROM modelle m
            JOIN marken mk ON m.marke_id = mk.id
            {where}
            ORDER BY mk.name, m.name
        """)

        total = len(modelle)
        stats = {'enriched': 0, 'errors': 0}

        logger.info("=" * 60)
        logger.info(f"LLM MODELL-ANREICHERUNG ({total} Modelle, Modell: {self.model})")
        logger.info("=" * 60)

        for i, modell in enumerate(modelle, 1):
            full_name = f"{modell['marke_name']} {modell['name']}"
            logger.info(f"[{i}/{total}] {full_name}")

            try:
                success = self._enrich_single_modell(modell)
                if success:
                    stats['enriched'] += 1
                else:
                    stats['errors'] += 1
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  -> Fehler: {e}", exc_info=True)

            time.sleep(self.REQUEST_DELAY)

        logger.info("=" * 60)
        logger.info(
            f"ERGEBNIS: {stats['enriched']} angereichert, "
            f"{stats['errors']} Fehler"
        )
        logger.info("=" * 60)
        return stats

    # ====================================================================
    #  MARKEN-ANREICHERUNG
    # ====================================================================

    def _enrich_single_marke(self, marke):
        """Reichert eine einzelne Marke mit LLM-Daten an."""
        name = marke['name']
        slug = marke['slug']
        marke_id = marke['id']

        # 1. LLM-Abfrage
        llm_data = self._ask_llm_for_marke(name)
        if not llm_data:
            return False

        # 2. Logo herunterladen (via Wikidata QID vom LLM)
        logo_url = None
        qid = llm_data.get('wikidata_id')
        if qid:
            props = self.wiki_importer._get_manufacturer_properties(qid)
            if props and props.get('logo_filename'):
                logo_url = self.wiki_importer._download_brand_logo(
                    props['logo_filename'], slug
                )
                time.sleep(0.3)

            # Wikipedia-URL auch holen
            wikipedia_url = props.get('wikipedia_url') if props else None
        else:
            wikipedia_url = None

        # 3. Datenbank aktualisieren
        updates = {}

        if llm_data.get('beschreibung'):
            updates['beschreibung'] = llm_data['beschreibung']
        if llm_data.get('gruendungsjahr'):
            updates['gruendungsjahr'] = llm_data['gruendungsjahr']
        if llm_data.get('herkunftsland'):
            updates['herkunftsland'] = llm_data['herkunftsland']
        if llm_data.get('website'):
            updates['website'] = llm_data['website']
        if qid:
            updates['wikidata_id'] = qid
        if wikipedia_url:
            updates['wikipedia_url'] = wikipedia_url
        if logo_url:
            updates['logo_url'] = logo_url

        if updates:
            set_parts = [f"{k} = %s" for k in updates]
            values = list(updates.values()) + [marke_id]
            db.execute(
                f"UPDATE marken SET {', '.join(set_parts)} WHERE id = %s",
                tuple(values)
            )

        fields = list(updates.keys())
        logger.info(f"  -> {qid or '?'}: {', '.join(fields)}")
        return True

    # ====================================================================
    #  MODELL-ANREICHERUNG
    # ====================================================================

    def _enrich_single_modell(self, modell):
        """Reichert ein einzelnes Modell mit LLM-Daten an."""
        name = modell['name']
        slug = modell['slug']
        marke_name = modell['marke_name']
        modell_id = modell['id']

        # 1. LLM-Abfrage
        llm_data = self._ask_llm_for_modell(marke_name, name)
        if not llm_data:
            return False

        # 2. Modellbild herunterladen (via Wikidata QID vom LLM)
        bild_url = None
        qid = llm_data.get('wikidata_id')
        if qid:
            props = self.wiki_importer._get_model_properties(qid)
            if props and props.get('image_filename'):
                bild_url = self.wiki_importer._download_model_image(
                    props['image_filename'], slug
                )
                time.sleep(0.3)

            wikipedia_url = props.get('wikipedia_url') if props else None
        else:
            wikipedia_url = None

        # 3. Datenbank aktualisieren
        updates = {}

        if llm_data.get('beschreibung'):
            updates['beschreibung'] = llm_data['beschreibung']
        if llm_data.get('bauzeit_von'):
            updates['bauzeit_von'] = llm_data['bauzeit_von']
        if llm_data.get('bauzeit_bis'):
            updates['bauzeit_bis'] = llm_data['bauzeit_bis']
        if llm_data.get('fahrzeugklasse'):
            updates['fahrzeugklasse'] = llm_data['fahrzeugklasse']
        if qid:
            updates['wikidata_id'] = qid
        if wikipedia_url:
            updates['wikipedia_url'] = wikipedia_url
        if bild_url:
            updates['bild_url'] = bild_url

        if updates:
            set_parts = [f"{k} = %s" for k in updates]
            values = list(updates.values()) + [modell_id]
            db.execute(
                f"UPDATE modelle SET {', '.join(set_parts)} WHERE id = %s",
                tuple(values)
            )

        fields = list(updates.keys())
        logger.info(f"  -> {qid or '?'}: {', '.join(fields)}")
        return True

    # ====================================================================
    #  LLM-API CALLS
    # ====================================================================

    def _ask_llm_for_marke(self, brand_name):
        """Fragt das LLM nach Informationen ueber eine Automarke."""
        return self._call_llm(
            SYSTEM_PROMPT_MARKE,
            f"Generiere Informationen fuer die Automarke: {brand_name}"
        )

    def _ask_llm_for_modell(self, brand_name, model_name):
        """Fragt das LLM nach Informationen ueber ein Automodell."""
        return self._call_llm(
            SYSTEM_PROMPT_MODELL,
            f"Generiere Informationen fuer das Automodell: {brand_name} {model_name}"
        )

    def _call_llm(self, system_prompt, user_prompt):
        """
        Fuehrt einen OpenAI API-Call durch und parst die JSON-Antwort.

        Returns:
            dict: Geparste JSON-Antwort oder None bei Fehler
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                response_format={"type": "json_object"},
                temperature=0.3,  # Niedrig fuer konsistente Fakten
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )

            content = response.choices[0].message.content
            if not content:
                logger.warning("  LLM gab leere Antwort zurueck")
                return None

            data = json.loads(content)

            # Basis-Validierung
            if not isinstance(data, dict):
                logger.warning(f"  LLM gab kein Dict zurueck: {type(data)}")
                return None

            # Wikidata-ID validieren (muss mit Q beginnen, gefolgt von Ziffern)
            qid = data.get('wikidata_id')
            if qid and not (
                isinstance(qid, str) and qid.startswith('Q') and qid[1:].isdigit()
            ):
                logger.warning(f"  Ungueltige Wikidata-ID: {qid}")
                data['wikidata_id'] = None

            # Nullwerte bereinigen (JSON null -> Python None)
            return {k: v for k, v in data.items() if v is not None}

        except json.JSONDecodeError as e:
            logger.error(f"  LLM JSON-Parsing fehlgeschlagen: {e}")
            return None
        except Exception as e:
            logger.error(f"  LLM API-Fehler: {e}")
            return None
