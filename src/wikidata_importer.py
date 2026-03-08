"""
Wikidata-Importer: Reichert Marken- und Modell-Daten mit Informationen
aus Wikidata und Wikipedia an.

Datenquellen:
- Wikidata SPARQL-Endpoint (CC0 Lizenz)
- Wikipedia REST API (CC-BY-SA Lizenz)
- Wikimedia Commons (CC-BY-SA Lizenz)

Bilder werden lokal heruntergeladen und ueber das Backend als statische
Dateien ausgeliefert (kein Hotlinking auf Wikimedia-Server).

Usage:
    python main.py --mode enrich-marken        # Marken anreichern
    python main.py --mode enrich-modelle       # Modelle anreichern
    python main.py --mode enrich-marken --force # Alle Marken neu anreichern
"""
import logging
import os
import time
import requests
from urllib.parse import quote, unquote
from unicodedata import normalize as unicode_normalize

from config import Config
from src.database import db

logger = logging.getLogger(__name__)


class WikidataImporter:
    """Importiert Stammdaten aus Wikidata und Wikipedia fuer Marken und Modelle.

    Bilder werden lokal heruntergeladen und als statische Dateien gespeichert.
    In der Datenbank werden relative URLs gespeichert (z.B. /images/marken/bmw.png).
    """

    SPARQL_URL = 'https://query.wikidata.org/sparql'
    WIKIDATA_API = 'https://www.wikidata.org/w/api.php'
    WIKIPEDIA_SUMMARY_API = 'https://de.wikipedia.org/api/rest_v1/page/summary'

    USER_AGENT = 'AlleZulassungenBot/1.0 (https://allezulassungen.de)'
    REQUEST_DELAY = 1.5  # Sekunden zwischen API-Aufrufen (Rate Limiting)

    # Logo-Breite fuer Thumbnails (Pixel)
    LOGO_WIDTH = 400
    # Modellbild-Breite fuer Hero-Images (Pixel)
    MODEL_IMAGE_WIDTH = 1200

    # Schluesselwoerter in Wikidata-Beschreibungen fuer Automobilhersteller
    AUTO_KEYWORDS = [
        # Deutsch
        'automobil', 'autohersteller', 'automarke', 'automobilhersteller',
        'fahrzeughersteller', 'nutzfahrzeug', 'kraftfahrzeug', 'motorrad',
        'automobilkonzern', 'autokonzern', 'autobauer',
        # Englisch
        'automobile', 'automotive', 'car manufacturer', 'car brand',
        'automaker', 'motor vehicle', 'vehicle manufacturer', 'car company',
        'truck manufacturer', 'motorcycle',
    ]

    # Schluesselwoerter fuer Automodelle
    MODEL_KEYWORDS = [
        # Deutsch
        'automodell', 'fahrzeugmodell', 'pkw', 'kleinwagen', 'mittelklasse',
        'oberklasse', 'gelaendewagen', 'sportwagen', 'limousine', 'kombi',
        'cabriolet', 'roadster', 'elektroauto', 'minivan', 'van',
        'baureihe', 'modellreihe',
        # Englisch
        'automobile model', 'car model', 'motor car', 'compact car', 'sedan',
        'suv', 'hatchback', 'coupe', 'station wagon', 'convertible',
        'electric car', 'crossover', 'pickup truck', 'subcompact',
        'model range', 'vehicle model', 'car manufactured',
    ]

    # Vollstaendige Wikidata-Zuordnungen fuer alle Automarken.
    # Manuell verifiziert, da LLMs Wikidata-QIDs halluzinieren und
    # die automatische Suche bei generischen Namen versagt.
    BRAND_OVERRIDES = {
        'Aiways': 'Q102066663',
        'Alfa Romeo': 'Q26257',
        'Alpine': 'Q743282',
        'Aston Martin': 'Q27074',
        'Audi': 'Q23317',
        'Bentley': 'Q466689',
        'BMW': 'Q26678',
        'BYD': 'Q4836753',       # BYD Auto (nicht BYD Company)
        'Cadillac': 'Q83776',
        'Chery': 'Q708898',
        'Citroën': 'Q6746',
        'Cupra': 'Q55662039',
        'Dacia': 'Q27460',
        'Daf Trucks': 'Q165200',
        'Deepal': 'Q112874951',
        'DS': 'Q2743308',
        'Ferrari': 'Q27586',
        'Fiat': 'Q27597',
        'Fisker': 'Q19592990',
        'Ford': 'Q44294',
        'Genesis': 'Q23900849',
        'GWM': 'Q207520',
        'Honda': 'Q9584',
        'Hyundai': 'Q55542',
        'Ineos': 'Q103131263',
        'Iveco': 'Q185073',
        'Jaecoo': 'Q124255702',
        'Jaguar': 'Q26913',
        'Jeep': 'Q19990',
        'Kgm': 'Q198096',       # KGM Motors (ehem. SsangYong)
        'KIA': 'Q42460',
        'Lada': 'Q37582',       # AvtoVAZ/Lada
        'Lamborghini': 'Q35476',
        'Lancia': 'Q18325',
        'Land Rover': 'Q26777',
        'Leapmotor': 'Q103374315',
        'Lexus': 'Q35919',
        'Lotus': 'Q175136',
        'Lucid': 'Q25328850',
        'Lynk & Co': 'Q27068706',
        'Man': 'Q206154',
        'Maserati': 'Q186071',
        'Maxus': 'Q1000802',
        'Mazda': 'Q35996',
        'Mercedes-Benz': 'Q36137',
        'Mg Roewe': 'Q186088',
        'Mini': 'Q116232',
        'Mitsubishi': 'Q36033',
        'Morgan': 'Q174424',
        'NIO': 'Q24520627',
        'Nissan': 'Q2790864',
        'Omoda': 'Q64139027',
        'Opel': 'Q40966',
        'Ora': 'Q97080490',
        'Peugeot': 'Q6742',
        'Polestar': 'Q37497810',
        'Porsche': 'Q40993',
        'Renault': 'Q6686',
        'Rolls-Royce': 'Q39747',
        'Seat': 'Q188307',
        'Skoda': 'Q29637',
        'Smart': 'Q156832',
        'Ssangyong': 'Q198096',
        'Subaru': 'Q172741',
        'Suzuki': 'Q181642',
        'Tesla': 'Q478214',
        'Togg': 'Q107193039',
        'Toyota': 'Q53268',
        'Vinfast': 'Q100540295',
        'Volkswagen': 'Q246',
        'Volvo': 'Q216652',
        'Xpeng': 'Q66069968',
        'Zeekr': 'Q107295112',
    }

    # Content-Type zu Dateiendung Mapping
    CONTENT_TYPE_EXT = {
        'image/svg+xml': 'svg',
        'image/png': 'png',
        'image/jpeg': 'jpg',
        'image/webp': 'webp',
        'image/gif': 'gif',
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.USER_AGENT,
            'Accept': 'application/json',
        })
        self.images_dir = Config.IMAGES_DIR
        self.images_base_url = Config.IMAGES_BASE_URL

        # Bilder-Verzeichnisse erstellen
        os.makedirs(os.path.join(self.images_dir, 'marken'), exist_ok=True)
        os.makedirs(os.path.join(self.images_dir, 'modelle'), exist_ok=True)
        logger.info(f"Bilder-Verzeichnis: {self.images_dir}")

    # ====================================================================
    #  OEFFENTLICHE METHODEN
    # ====================================================================

    def enrich_marken(self, force=False):
        """
        Reichert alle Marken in der Datenbank mit Wikidata/Wikipedia-Daten an.
        Bilder werden lokal heruntergeladen.

        Args:
            force: Wenn True, werden auch bereits angereicherte Marken aktualisiert.

        Returns:
            dict: Statistik mit 'enriched', 'not_found', 'errors'
        """
        where = "" if force else "WHERE wikidata_id IS NULL"
        marken = db.execute(
            f"SELECT id, name, slug, wikidata_id FROM marken {where} ORDER BY name"
        )

        total = len(marken)
        stats = {'enriched': 0, 'not_found': 0, 'errors': 0}

        logger.info("=" * 60)
        logger.info(f"WIKIDATA MARKEN-ANREICHERUNG ({total} Marken)")
        logger.info("=" * 60)

        for i, marke in enumerate(marken, 1):
            name = marke['name']
            logger.info(f"[{i}/{total}] {name}")

            try:
                success = self._enrich_single_marke(marke)
                if success:
                    stats['enriched'] += 1
                else:
                    stats['not_found'] += 1
                    logger.warning(f"  -> Nicht gefunden auf Wikidata")
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  -> Fehler: {e}", exc_info=True)

            time.sleep(self.REQUEST_DELAY)

        logger.info("=" * 60)
        logger.info(
            f"ERGEBNIS: {stats['enriched']} angereichert, "
            f"{stats['not_found']} nicht gefunden, "
            f"{stats['errors']} Fehler"
        )
        logger.info("=" * 60)
        return stats

    def enrich_modelle(self, force=False):
        """
        Reichert alle Modelle in der Datenbank mit Wikidata/Wikipedia-Daten an.
        Bilder werden lokal heruntergeladen.

        Args:
            force: Wenn True, werden auch bereits angereicherte Modelle aktualisiert.

        Returns:
            dict: Statistik mit 'enriched', 'not_found', 'errors'
        """
        where = "WHERE m.wikidata_id IS NULL" if not force else ""
        modelle = db.execute(f"""
            SELECT m.id, m.name, m.slug, m.wikidata_id,
                   mk.name AS marke_name, mk.slug AS marke_slug,
                   mk.wikidata_id AS marke_wikidata_id
            FROM modelle m
            JOIN marken mk ON m.marke_id = mk.id
            {where}
            ORDER BY mk.name, m.name
        """)

        total = len(modelle)
        stats = {'enriched': 0, 'not_found': 0, 'errors': 0}

        logger.info("=" * 60)
        logger.info(f"WIKIDATA MODELL-ANREICHERUNG ({total} Modelle)")
        logger.info("=" * 60)

        for i, modell in enumerate(modelle, 1):
            full_name = f"{modell['marke_name']} {modell['name']}"
            logger.info(f"[{i}/{total}] {full_name}")

            try:
                success = self._enrich_single_modell(modell)
                if success:
                    stats['enriched'] += 1
                else:
                    stats['not_found'] += 1
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  -> Fehler: {e}", exc_info=True)

            time.sleep(self.REQUEST_DELAY)

        logger.info("=" * 60)
        logger.info(
            f"ERGEBNIS: {stats['enriched']} angereichert, "
            f"{stats['not_found']} nicht gefunden, "
            f"{stats['errors']} Fehler"
        )
        logger.info("=" * 60)
        return stats

    def fix_brand_logos(self):
        """
        Repariert Marken-Logos: Verwendet die verifizierten BRAND_OVERRIDES
        QIDs und laedt NUR P154 (Logo-Property) von Wikidata herunter.

        Loest das Problem, dass:
        - LLMs falsche Wikidata-QIDs halluzinieren
        - P18 (allgemeines Bild) Personen/Gebaeude statt Logos zeigt

        Returns:
            dict: Statistik mit 'fixed', 'no_logo', 'errors'
        """
        marken = db.execute(
            "SELECT id, name, slug, wikidata_id FROM marken ORDER BY name"
        )

        total = len(marken)
        stats = {'fixed': 0, 'no_logo': 0, 'errors': 0}

        logger.info("=" * 60)
        logger.info(f"LOGO-REPARATUR ({total} Marken)")
        logger.info("=" * 60)

        # Alte Logo-Dateien loeschen
        logo_dir = os.path.join(self.images_dir, 'marken')
        for f in os.listdir(logo_dir):
            filepath = os.path.join(logo_dir, f)
            if os.path.isfile(filepath):
                os.remove(filepath)
                logger.debug(f"  Geloescht: {f}")
        logger.info(f"Alte Logos geloescht aus {logo_dir}")

        for i, marke in enumerate(marken, 1):
            name = marke['name']
            slug = marke['slug']
            marke_id = marke['id']
            logger.info(f"[{i}/{total}] {name}")

            try:
                # 1. Korrekte QID aus BRAND_OVERRIDES (verifiziert!)
                qid = self.BRAND_OVERRIDES.get(name)
                if not qid:
                    # Fallback: Wikidata-Suche (fuer unbekannte Marken)
                    qid = self._find_manufacturer_entity(name)

                if not qid:
                    stats['no_logo'] += 1
                    logger.warning(f"  -> Keine Wikidata-Entitaet gefunden")
                    continue

                # 2. NUR Logo (P154) abfragen - KEIN P18-Fallback
                logo_filename = self._get_logo_only(qid)

                # Fallback: Suche nach JEDER Entitaet mit diesem Namen + Logo
                if not logo_filename:
                    logger.debug(f"  -> {qid}: Kein P154, versuche Namenssuche...")
                    alt_qid, logo_filename = self._search_logo_by_name(name)
                    if alt_qid:
                        logger.info(f"  -> Logo gefunden via Namenssuche: {alt_qid}")
                    time.sleep(0.5)

                if not logo_filename:
                    stats['no_logo'] += 1
                    logger.info(f"  -> {qid}: Kein Logo gefunden")
                    # Wikidata-ID trotzdem korrigieren
                    db.execute(
                        "UPDATE marken SET wikidata_id = %s, logo_url = NULL WHERE id = %s",
                        (qid, marke_id)
                    )
                    continue

                # 3. Logo herunterladen
                logo_url = self._download_brand_logo(logo_filename, slug)
                time.sleep(0.5)

                # 4. DB aktualisieren (auch Wikidata-ID korrigieren!)
                db.execute(
                    "UPDATE marken SET wikidata_id = %s, logo_url = %s WHERE id = %s",
                    (qid, logo_url, marke_id)
                )

                stats['fixed'] += 1
                logger.info(f"  -> {qid}: {logo_url}")

            except Exception as e:
                stats['errors'] += 1
                logger.error(f"  -> Fehler: {e}", exc_info=True)

            time.sleep(0.5)

        logger.info("=" * 60)
        logger.info(
            f"ERGEBNIS: {stats['fixed']} Logos heruntergeladen, "
            f"{stats['no_logo']} ohne Logo, "
            f"{stats['errors']} Fehler"
        )
        logger.info("=" * 60)
        return stats

    def _get_logo_only(self, qid):
        """
        Holt NUR das Logo (P154) von Wikidata, KEIN Fallback auf P18.

        Returns:
            str: Dateiname des Logos auf Wikimedia Commons oder None
        """
        query = f"""
        SELECT ?logo WHERE {{
          wd:{qid} wdt:P154 ?logo .
        }}
        LIMIT 1
        """
        try:
            resp = self.session.get(
                self.SPARQL_URL,
                params={'query': query, 'format': 'json'},
            )
            resp.raise_for_status()

            bindings = resp.json().get('results', {}).get('bindings', [])
            if bindings and 'logo' in bindings[0]:
                return self._extract_commons_filename(
                    bindings[0]['logo']['value']
                )
            return None
        except requests.RequestException as e:
            logger.error(f"SPARQL Logo-Abfrage fehlgeschlagen: {e}")
            return None

    def _search_logo_by_name(self, brand_name):
        """
        Sucht ueber den Markennamen nach JEDER Wikidata-Entitaet mit Logo (P154).
        Fallback wenn der primaere QID kein Logo hat.

        Durchsucht sowohl die Marken-Entitaet als auch Firmen-Entitaeten,
        da manche Logos nur auf der Firma (nicht der Marke) eingetragen sind.

        Returns:
            tuple: (qid, logo_filename) oder (None, None)
        """
        # Suchvarianten generieren
        search_names = self._get_name_variants(brand_name)

        for name in search_names:
            for lang in ['de', 'en']:
                query = f"""
                SELECT ?item ?logo WHERE {{
                  ?item rdfs:label "{name}"@{lang} .
                  ?item wdt:P154 ?logo .
                }}
                LIMIT 3
                """
                try:
                    resp = self.session.get(
                        self.SPARQL_URL,
                        params={'query': query, 'format': 'json'},
                    )
                    resp.raise_for_status()

                    bindings = resp.json().get('results', {}).get('bindings', [])
                    for b in bindings:
                        if 'logo' in b and 'item' in b:
                            qid = b['item']['value'].split('/')[-1]
                            filename = self._extract_commons_filename(
                                b['logo']['value']
                            )
                            return qid, filename

                    time.sleep(0.3)
                except requests.RequestException as e:
                    logger.debug(f"Logo-Suche fehlgeschlagen fuer '{name}': {e}")

        return None, None

    # ====================================================================
    #  MARKEN-ANREICHERUNG (INTERN)
    # ====================================================================

    def _enrich_single_marke(self, marke):
        """Sucht und speichert Wikidata-Infos fuer eine einzelne Marke."""
        name = marke['name']
        slug = marke['slug']
        marke_id = marke['id']

        # 1. Wikidata-Entitaet finden
        qid = self.BRAND_OVERRIDES.get(name)
        if not qid:
            qid = self._find_manufacturer_entity(name)
        if not qid:
            return False

        # 2. Properties via SPARQL abfragen
        props = self._get_manufacturer_properties(qid)

        # 3. Logo herunterladen
        logo_url = None
        if props and props.get('logo_filename'):
            logo_url = self._download_brand_logo(
                props['logo_filename'], slug
            )
            time.sleep(0.5)

        # 4. Wikipedia-Zusammenfassung holen
        beschreibung = None
        wikipedia_url = props.get('wikipedia_url') if props else None
        if wikipedia_url:
            time.sleep(0.5)
            beschreibung = self._fetch_wikipedia_summary(wikipedia_url)

        # 5. Datenbank aktualisieren
        updates = {'wikidata_id': qid}

        if wikipedia_url:
            updates['wikipedia_url'] = wikipedia_url
        if logo_url:
            updates['logo_url'] = logo_url
        if props:
            if props.get('inception_year'):
                updates['gruendungsjahr'] = props['inception_year']
            if props.get('country_code'):
                updates['herkunftsland'] = props['country_code']
            if props.get('website'):
                updates['website'] = props['website']
        if beschreibung:
            updates['beschreibung'] = beschreibung

        self._update_marke(marke_id, updates)

        # Logging: welche Felder wurden gefuellt?
        fields = [k for k in updates if k != 'wikidata_id' and updates[k]]
        logger.info(f"  -> {qid}: {', '.join(fields) if fields else 'nur Wikidata-ID'}")
        return True

    # ====================================================================
    #  MODELL-ANREICHERUNG (INTERN)
    # ====================================================================

    def _enrich_single_modell(self, modell):
        """Sucht und speichert Wikidata-Infos fuer ein einzelnes Modell."""
        name = modell['name']
        slug = modell['slug']
        marke_name = modell['marke_name']
        modell_id = modell['id']

        # 1. Wikidata-Entitaet finden
        qid = self._find_model_entity(name, marke_name)
        if not qid:
            return False

        # 2. Properties via SPARQL abfragen
        props = self._get_model_properties(qid)

        # 3. Modellbild herunterladen
        bild_url = None
        if props and props.get('image_filename'):
            bild_url = self._download_model_image(
                props['image_filename'], slug
            )
            time.sleep(0.5)

        # 4. Wikipedia-Zusammenfassung holen
        beschreibung = None
        wikipedia_url = props.get('wikipedia_url') if props else None
        if wikipedia_url:
            time.sleep(0.5)
            beschreibung = self._fetch_wikipedia_summary(wikipedia_url)

        # 5. Datenbank aktualisieren
        updates = {'wikidata_id': qid}

        if wikipedia_url:
            updates['wikipedia_url'] = wikipedia_url
        if bild_url:
            updates['bild_url'] = bild_url
        if props:
            if props.get('production_start'):
                updates['bauzeit_von'] = props['production_start']
            if props.get('production_end'):
                updates['bauzeit_bis'] = props['production_end']
            if props.get('vehicle_class'):
                updates['fahrzeugklasse'] = props['vehicle_class']
        if beschreibung:
            updates['beschreibung'] = beschreibung

        self._update_modell(modell_id, updates)

        fields = [k for k in updates if k != 'wikidata_id' and updates[k]]
        logger.info(f"  -> {qid}: {', '.join(fields) if fields else 'nur Wikidata-ID'}")
        return True

    # ====================================================================
    #  BILD-DOWNLOAD
    # ====================================================================

    def _download_brand_logo(self, commons_filename, brand_slug):
        """
        Laedt ein Marken-Logo von Wikimedia Commons herunter.

        Args:
            commons_filename: Dateiname auf Wikimedia Commons (z.B. 'BMW.svg')
            brand_slug: URL-Slug der Marke (z.B. 'bmw')

        Returns:
            str: Relativer URL-Pfad (z.B. '/images/marken/bmw.png') oder None
        """
        return self._download_commons_image(
            commons_filename,
            subdir='marken',
            local_name=brand_slug,
            width=self.LOGO_WIDTH,
        )

    def _download_model_image(self, commons_filename, model_slug):
        """
        Laedt ein Modell-Bild von Wikimedia Commons herunter.

        Args:
            commons_filename: Dateiname auf Wikimedia Commons
            model_slug: URL-Slug des Modells (z.B. 'volkswagen-golf')

        Returns:
            str: Relativer URL-Pfad (z.B. '/images/modelle/volkswagen-golf.jpg') oder None
        """
        return self._download_commons_image(
            commons_filename,
            subdir='modelle',
            local_name=model_slug,
            width=self.MODEL_IMAGE_WIDTH,
        )

    def _download_commons_image(self, commons_filename, subdir, local_name, width):
        """
        Generische Methode zum Herunterladen eines Bildes von Wikimedia Commons.

        Das Bild wird als Thumbnail in der angegebenen Breite heruntergeladen.
        SVG-Dateien werden dabei automatisch zu PNG konvertiert.

        Args:
            commons_filename: Original-Dateiname auf Commons
            subdir: Unterverzeichnis (z.B. 'marken' oder 'modelle')
            local_name: Lokaler Dateiname ohne Extension
            width: Thumbnail-Breite in Pixeln

        Returns:
            str: Relativer URL-Pfad oder None bei Fehler
        """
        # Thumbnail-URL erstellen
        encoded_filename = quote(commons_filename.replace(' ', '_'))
        thumb_url = (
            f"https://commons.wikimedia.org/wiki/Special:FilePath/"
            f"{encoded_filename}?width={width}"
        )

        try:
            resp = self.session.get(
                thumb_url,
                stream=True,
                allow_redirects=True,
                timeout=30,
                headers={'User-Agent': self.USER_AGENT},
            )
            resp.raise_for_status()

            # Dateiendung aus Content-Type bestimmen
            content_type = resp.headers.get('Content-Type', '')
            content_type_clean = content_type.split(';')[0].strip().lower()
            ext = self.CONTENT_TYPE_EXT.get(content_type_clean, 'png')

            # Datei speichern
            save_dir = os.path.join(self.images_dir, subdir)
            filepath = os.path.join(save_dir, f"{local_name}.{ext}")

            with open(filepath, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = os.path.getsize(filepath)
            logger.debug(
                f"  Bild gespeichert: {filepath} "
                f"({file_size / 1024:.1f} KB, {ext})"
            )

            # Relativen URL-Pfad zurueckgeben
            return f"{self.images_base_url}/{subdir}/{local_name}.{ext}"

        except requests.RequestException as e:
            logger.warning(f"  Bild-Download fehlgeschlagen ({commons_filename}): {e}")
            return None
        except IOError as e:
            logger.warning(f"  Bild speichern fehlgeschlagen ({local_name}): {e}")
            return None

    # ====================================================================
    #  WIKIDATA-SUCHE
    # ====================================================================

    def _find_manufacturer_entity(self, brand_name):
        """
        Findet die passende Wikidata-Entitaet fuer einen Automobilhersteller.

        Strategie:
        1. Name-Varianten generieren (mit/ohne Akzente)
        2. Wikidata in DE und EN durchsuchen
        3. Kandidaten nach Automobil-Relevanz bewerten
        4. Besten Kandidaten zurueckgeben
        """
        search_variants = self._get_name_variants(brand_name)

        for variant in search_variants:
            for lang in ['de', 'en']:
                candidates = self._wikidata_search(variant, lang)
                best = self._pick_best_candidate(
                    candidates, brand_name, self.AUTO_KEYWORDS, min_score=2
                )
                if best:
                    return best
                time.sleep(0.3)

        return None

    def _find_model_entity(self, model_name, brand_name):
        """
        Findet die passende Wikidata-Entitaet fuer ein Automodell.

        Strategie:
        1. Suche nach "Marke Modell" Kombination
        2. Suche nach Modellname allein
        3. Kandidaten nach Modell-Relevanz bewerten
        """
        search_terms = [
            f"{brand_name} {model_name}",
            model_name,
        ]

        for term in search_terms:
            for lang in ['de', 'en']:
                candidates = self._wikidata_search(term, lang)

                for candidate in candidates:
                    desc = (candidate.get('description', '') or '').lower()
                    label = (candidate.get('label', '') or '').lower()

                    # Beschreibung muss Modell-Keywords enthalten
                    has_model_keyword = any(
                        kw in desc for kw in self.MODEL_KEYWORDS
                    )
                    # Label muss den Modellnamen enthalten
                    name_match = (
                        model_name.lower() in label
                        or label in model_name.lower()
                    )
                    # Oder Markenname in Beschreibung + Modellname im Label
                    brand_in_desc = brand_name.lower() in desc

                    if (has_model_keyword and name_match) or \
                       (brand_in_desc and name_match):
                        return candidate['id']

                time.sleep(0.3)

        return None

    def _wikidata_search(self, query, language='de'):
        """Sucht Wikidata-Entitaeten per Name."""
        params = {
            'action': 'wbsearchentities',
            'search': query,
            'language': language,
            'type': 'item',
            'limit': 5,
            'format': 'json',
        }
        try:
            resp = self.session.get(self.WIKIDATA_API, params=params)
            resp.raise_for_status()
            return resp.json().get('search', [])
        except requests.RequestException as e:
            logger.warning(f"Wikidata-Suche fehlgeschlagen fuer '{query}': {e}")
            return []

    def _pick_best_candidate(self, candidates, expected_name, keywords, min_score=2):
        """
        Waehlt den besten Kandidaten basierend auf Namens-Uebereinstimmung
        und Relevanz-Keywords in der Beschreibung.
        """
        if not candidates:
            return None

        best_qid = None
        best_score = 0

        for c in candidates:
            score = self._score_candidate(c, expected_name, keywords)
            if score > best_score:
                best_score = score
                best_qid = c['id']

        return best_qid if best_score >= min_score else None

    def _score_candidate(self, candidate, expected_name, keywords):
        """
        Bewertet einen Wikidata-Kandidaten.

        Punkte:
        - Exakte Namens-Uebereinstimmung: +3
        - Teilweise Namens-Uebereinstimmung: +1
        - Relevantes Keyword in Beschreibung: +2
        """
        score = 0
        label = (candidate.get('label', '') or '').lower()
        desc = (candidate.get('description', '') or '').lower()

        expected_lower = expected_name.lower()

        # Namens-Uebereinstimmung
        if label == expected_lower:
            score += 3
        elif expected_lower in label or label in expected_lower:
            score += 1

        # Keyword in Beschreibung
        for kw in keywords:
            if kw in desc:
                score += 2
                break  # Nur einmal zaehlen

        return score

    def _get_name_variants(self, name):
        """Generiert Suchvarianten fuer einen Namen (z.B. ohne Akzente)."""
        variants = [name]

        # ASCII-Variante (Citroën -> Citroen)
        ascii_name = unicode_normalize('NFKD', name).encode(
            'ASCII', 'ignore'
        ).decode('ASCII')
        if ascii_name != name and ascii_name:
            variants.append(ascii_name)

        return variants

    # ====================================================================
    #  SPARQL-ABFRAGEN
    # ====================================================================

    def _get_manufacturer_properties(self, qid):
        """
        Holt Hersteller-Properties via Wikidata SPARQL.

        Abgefragte Properties:
        - P154: Logo
        - P18:  Bild (Fallback fuer Logo)
        - P571: Gruendungsjahr
        - P17:  Land -> P297: ISO-Code
        - P856: Website
        - P159: Hauptsitz
        - Wikipedia-Artikel (de)
        """
        query = f"""
        SELECT ?logo ?image ?inception ?countryCode ?website
               ?headquartersLabel ?article
        WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P154 ?logo }}
          OPTIONAL {{ wd:{qid} wdt:P18 ?image }}
          OPTIONAL {{ wd:{qid} wdt:P571 ?inception }}
          OPTIONAL {{
            wd:{qid} wdt:P17 ?country .
            ?country wdt:P297 ?countryCode .
          }}
          OPTIONAL {{ wd:{qid} wdt:P856 ?website }}
          OPTIONAL {{ wd:{qid} wdt:P159 ?headquarters }}
          OPTIONAL {{
            ?article schema:about wd:{qid} ;
                     schema:isPartOf <https://de.wikipedia.org/> .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en" }}
        }}
        LIMIT 1
        """
        return self._execute_sparql(query, self._parse_manufacturer_result)

    def _get_model_properties(self, qid):
        """
        Holt Modell-Properties via Wikidata SPARQL.

        Abgefragte Properties:
        - P18:  Bild
        - P571: Produktionsstart
        - P576: Produktionsende
        - P279: Fahrzeugklasse (Oberklasse)
        - Wikipedia-Artikel (de)
        """
        query = f"""
        SELECT ?image ?inception ?discontinued ?vehicleClassLabel ?article
        WHERE {{
          OPTIONAL {{ wd:{qid} wdt:P18 ?image }}
          OPTIONAL {{ wd:{qid} wdt:P571 ?inception }}
          OPTIONAL {{ wd:{qid} wdt:P576 ?discontinued }}
          OPTIONAL {{ wd:{qid} wdt:P279 ?vehicleClass }}
          OPTIONAL {{
            ?article schema:about wd:{qid} ;
                     schema:isPartOf <https://de.wikipedia.org/> .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "de,en" }}
        }}
        LIMIT 1
        """
        return self._execute_sparql(query, self._parse_model_result)

    def _execute_sparql(self, query, parser_func):
        """Fuehrt eine SPARQL-Abfrage aus und parst das Ergebnis."""
        try:
            resp = self.session.get(
                self.SPARQL_URL,
                params={'query': query, 'format': 'json'},
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"SPARQL-Fehler: {e}")
            return None

        bindings = resp.json().get('results', {}).get('bindings', [])
        if not bindings:
            return None

        return parser_func(bindings[0])

    def _parse_manufacturer_result(self, row):
        """Parst das SPARQL-Ergebnis fuer einen Hersteller."""
        props = {}

        # Logo (P154) - NUR das echte Logo, kein Fallback auf P18!
        # P18 (allgemeines Bild) zeigt oft Personen, Gebaeude oder
        # historische Fotos statt des Markenlogos.
        if 'logo' in row:
            props['logo_filename'] = self._extract_commons_filename(
                row['logo']['value']
            )

        # Gruendungsjahr
        if 'inception' in row:
            year = self._extract_year(row['inception']['value'])
            if year and 1800 <= year <= 2030:  # Plausibilitaetspruefung
                props['inception_year'] = year

        # Herkunftsland (ISO-Code)
        if 'countryCode' in row:
            code = row['countryCode']['value']
            if len(code) == 2:  # Gueltige ISO-Codes
                props['country_code'] = code

        # Website
        if 'website' in row:
            props['website'] = row['website']['value']

        # Hauptsitz
        if 'headquartersLabel' in row:
            hq = row['headquartersLabel']['value']
            if not hq.startswith('Q'):  # Ungeparste QIDs ignorieren
                props['headquarters'] = hq

        # Wikipedia-Artikel
        if 'article' in row:
            props['wikipedia_url'] = row['article']['value']

        return props

    def _parse_model_result(self, row):
        """Parst das SPARQL-Ergebnis fuer ein Modell."""
        props = {}

        # Bild
        if 'image' in row:
            props['image_filename'] = self._extract_commons_filename(
                row['image']['value']
            )

        # Produktionszeitraum
        if 'inception' in row:
            year = self._extract_year(row['inception']['value'])
            if year and 1900 <= year <= 2030:
                props['production_start'] = year
        if 'discontinued' in row:
            year = self._extract_year(row['discontinued']['value'])
            if year and 1900 <= year <= 2030:
                props['production_end'] = year

        # Fahrzeugklasse
        if 'vehicleClassLabel' in row:
            vc = row['vehicleClassLabel']['value']
            if not vc.startswith('Q'):
                props['vehicle_class'] = vc

        # Wikipedia-Artikel
        if 'article' in row:
            props['wikipedia_url'] = row['article']['value']

        return props

    # ====================================================================
    #  WIKIPEDIA
    # ====================================================================

    def _fetch_wikipedia_summary(self, url):
        """
        Holt die Wikipedia-Zusammenfassung fuer einen Artikel.

        Args:
            url: Volle Wikipedia-URL (z.B. https://de.wikipedia.org/wiki/BMW)

        Returns:
            str: Zusammenfassung als Plaintext oder None
        """
        try:
            # Titel aus URL extrahieren: https://de.wikipedia.org/wiki/Title
            title = url.split('/wiki/')[-1]
            resp = self.session.get(
                f"{self.WIKIPEDIA_SUMMARY_API}/{title}"
            )
            resp.raise_for_status()
            data = resp.json()
            extract = data.get('extract')

            if extract and len(extract) > 20:
                return extract
            return None
        except Exception as e:
            logger.warning(f"Wikipedia-Zusammenfassung nicht verfuegbar: {e}")
            return None

    # ====================================================================
    #  HILFSFUNKTIONEN
    # ====================================================================

    def _extract_commons_filename(self, url):
        """Extrahiert den Dateinamen aus einer Wikimedia Commons URL."""
        if 'Special:FilePath/' in url:
            return unquote(url.split('Special:FilePath/')[-1])
        return unquote(url.split('/')[-1])

    def _extract_year(self, date_string):
        """Extrahiert das Jahr aus einem Wikidata-Datum (z.B. 1937-01-01T00:00:00Z)."""
        try:
            return int(date_string[:4])
        except (ValueError, IndexError, TypeError):
            return None

    def _update_marke(self, marke_id, updates):
        """Aktualisiert eine Marke in der Datenbank (nur nicht-leere Werte)."""
        if not updates:
            return

        filtered = {
            k: v for k, v in updates.items()
            if v is not None or k == 'wikidata_id'
        }

        set_parts = [f"{k} = %s" for k in filtered]
        values = list(filtered.values()) + [marke_id]

        db.execute(
            f"UPDATE marken SET {', '.join(set_parts)} WHERE id = %s",
            tuple(values)
        )

    def _update_modell(self, modell_id, updates):
        """Aktualisiert ein Modell in der Datenbank (nur nicht-leere Werte)."""
        if not updates:
            return

        filtered = {
            k: v for k, v in updates.items()
            if v is not None or k == 'wikidata_id'
        }

        set_parts = [f"{k} = %s" for k in filtered]
        values = list(filtered.values()) + [modell_id]

        db.execute(
            f"UPDATE modelle SET {', '.join(set_parts)} WHERE id = %s",
            tuple(values)
        )
