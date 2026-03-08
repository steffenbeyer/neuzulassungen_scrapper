"""
Normalizer: Vereinheitlicht Markennamen, Modellbezeichnungen und Kraftstofftypen.
Das KBA hat ueber die Jahre Format-Aenderungen vorgenommen, die hier ausgeglichen werden.
"""
import re
import logging
from unicodedata import normalize as unicode_normalize

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Normalisiert KBA-Daten fuer konsistente Speicherung."""

    # Bekannte Marken-Aliase (KBA-Name -> Normalisierter Name)
    MARKEN_ALIASE = {
        'VW': 'VOLKSWAGEN',
        'VOLKSWAGEN/VW': 'VOLKSWAGEN',
        'VW/VOLKSWAGEN': 'VOLKSWAGEN',
        'MB': 'MERCEDES-BENZ',
        'MERCEDES BENZ': 'MERCEDES-BENZ',
        'MERCEDES': 'MERCEDES-BENZ',
        'BMW/MINI': 'BMW',
        'AUDI/VW': 'AUDI',
        'GM/OPEL': 'OPEL',
        'PSA/OPEL': 'OPEL',
        'STELLANTIS/OPEL': 'OPEL',
        'STELLANTIS/PEUGEOT': 'PEUGEOT',
        'STELLANTIS/CITROEN': 'CITROËN',
        'CITROËN': 'CITROËN',
        'CITROEN': 'CITROËN',
        'ALFA': 'ALFA ROMEO',
        'ROLLS ROYCE': 'ROLLS-ROYCE',
        'ASTON MARTIN': 'ASTON MARTIN',
        'LAND ROVER': 'LAND ROVER',
        'RANGE ROVER': 'LAND ROVER',
    }

    # Kraftstoff-Normalisierung
    KRAFTSTOFF_MAP = {
        'BENZIN': 'Benzin',
        'OTTOMOTOR': 'Benzin',
        'OTTO': 'Benzin',
        'DIESEL': 'Diesel',
        'DIESELMOTOR': 'Diesel',
        'ELEKTRO': 'Elektro',
        'ELEKTR.': 'Elektro',
        'ELEKTRISCH': 'Elektro',
        'BEV': 'Elektro',
        'HYBRID': 'Hybrid',
        'PLUG-IN-HYBRID': 'Plug-in-Hybrid',
        'PHEV': 'Plug-in-Hybrid',
        'PLUG-IN HYBRID': 'Plug-in-Hybrid',
        'HYBRID-BENZIN': 'Hybrid-Benzin',
        'HYBRID BENZIN': 'Hybrid-Benzin',
        'HYBRID-DIESEL': 'Hybrid-Diesel',
        'HYBRID DIESEL': 'Hybrid-Diesel',
        'GAS': 'Gas',
        'ERDGAS': 'Erdgas/CNG',
        'CNG': 'Erdgas/CNG',
        'LPG': 'Autogas/LPG',
        'AUTOGAS': 'Autogas/LPG',
        'FLÜSSIGGAS': 'Autogas/LPG',
        'WASSERSTOFF': 'Wasserstoff',
        'H2': 'Wasserstoff',
        'FCEV': 'Wasserstoff',
        'INSGESAMT': 'Insgesamt',
        'GESAMT': 'Insgesamt',
        'ZUSAMMEN': 'Insgesamt',
    }

    @staticmethod
    def normalize_marke(name):
        """Normalisiert einen Markennamen."""
        if not name:
            return None

        # Leerzeichen bereinigen, Grossbuchstaben
        cleaned = ' '.join(name.strip().upper().split())

        # Bekannte Aliase pruefen
        if cleaned in DataNormalizer.MARKEN_ALIASE:
            cleaned = DataNormalizer.MARKEN_ALIASE[cleaned]

        # Titel-Format: Erster Buchstabe gross, Rest klein (mit Ausnahmen)
        exceptions = {'BMW', 'VW', 'MG', 'DS', 'BYD', 'KIA', 'JAC', 'GAC', 'GWM', 'NIO'}
        if cleaned in exceptions:
            return cleaned

        def _title_word(word):
            """Einzelnes Wort titeln, Bindestriche beruecksichtigen."""
            if '-' in word:
                return '-'.join(p.capitalize() for p in word.split('-'))
            return word.capitalize()

        parts = cleaned.split(' ')
        result = ' '.join(_title_word(p) for p in parts)

        return result

    @staticmethod
    def normalize_modell(name):
        """Normalisiert einen Modellnamen."""
        if not name:
            return None

        # Leerzeichen bereinigen
        cleaned = ' '.join(name.strip().split())

        # Fuehrende/nachfolgende Sonderzeichen entfernen
        cleaned = cleaned.strip('*-_ ')

        # Leere Namen filtern
        if not cleaned or cleaned in ('SONSTIGE', 'SONSTIGE MODELLE', 'SONSTIGES',
                                       'ÜBRIGE', 'INSGESAMT', 'ZUSAMMEN'):
            return None

        return cleaned

    @staticmethod
    def normalize_kraftstoff(name):
        """Normalisiert eine Kraftstoffbezeichnung."""
        if not name:
            return None

        cleaned = name.strip().upper()

        if cleaned in DataNormalizer.KRAFTSTOFF_MAP:
            return DataNormalizer.KRAFTSTOFF_MAP[cleaned]

        # Teilstring-Matching
        for key, value in DataNormalizer.KRAFTSTOFF_MAP.items():
            if key in cleaned:
                return value

        logger.warning(f"Unbekannter Kraftstoff: '{name}'")
        return name.strip()

    @staticmethod
    def generate_slug(name):
        """Generiert einen URL-freundlichen Slug aus einem Namen."""
        if not name:
            return ''

        # Unicode normalisieren
        slug = unicode_normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')

        # Kleinbuchstaben
        slug = slug.lower()

        # Sonderzeichen durch Bindestriche ersetzen
        slug = re.sub(r'[^a-z0-9]+', '-', slug)

        # Fuehrende/nachfolgende Bindestriche entfernen
        slug = slug.strip('-')

        # Doppelte Bindestriche entfernen
        slug = re.sub(r'-+', '-', slug)

        return slug

    @staticmethod
    def normalize_anzahl(value):
        """Konvertiert einen Zulassungswert in eine Ganzzahl."""
        if value is None:
            return 0

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):
            # Tausender-Punkte und Leerzeichen entfernen
            cleaned = value.replace('.', '').replace(',', '').replace(' ', '').strip()
            if cleaned == '' or cleaned == '-' or cleaned == '...':
                return 0
            try:
                return int(cleaned)
            except ValueError:
                logger.warning(f"Kann Anzahl nicht konvertieren: '{value}'")
                return 0

        return 0
