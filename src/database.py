"""
Datenbank-Verbindung und Hilfsfunktionen fuer MariaDB.
"""
import logging
import pymysql
from pymysql.cursors import DictCursor
from config import Config

logger = logging.getLogger(__name__)


class Database:
    """Verwaltet die MariaDB-Verbindung."""

    def __init__(self):
        self._connection = None

    def connect(self):
        """Stellt eine Verbindung zur Datenbank her."""
        if self._connection and self._connection.open:
            return self._connection

        try:
            self._connection = pymysql.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                database=Config.DB_NAME,
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=False
            )
            logger.info(f"Datenbankverbindung hergestellt: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
            return self._connection
        except pymysql.Error as e:
            logger.error(f"Datenbankverbindung fehlgeschlagen: {e}")
            raise

    def close(self):
        """Schliesst die Datenbankverbindung."""
        if self._connection and self._connection.open:
            self._connection.close()
            logger.info("Datenbankverbindung geschlossen.")

    def get_connection(self):
        """Gibt die aktuelle Verbindung zurueck oder erstellt eine neue."""
        if not self._connection or not self._connection.open:
            self.connect()
        return self._connection

    def execute(self, query, params=None):
        """Fuehrt eine SQL-Abfrage aus und gibt die Ergebnisse zurueck."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.fetchall()
        except pymysql.Error as e:
            conn.rollback()
            logger.error(f"SQL-Fehler: {e}\nQuery: {query}\nParams: {params}")
            raise

    def execute_many(self, query, params_list):
        """Fuehrt eine SQL-Abfrage fuer mehrere Datensaetze aus."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount
        except pymysql.Error as e:
            conn.rollback()
            logger.error(f"SQL-Fehler (executemany): {e}\nQuery: {query}")
            raise

    def insert_or_update(self, query, params=None):
        """Fuehrt ein INSERT mit ON DUPLICATE KEY UPDATE aus."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.lastrowid
        except pymysql.Error as e:
            conn.rollback()
            logger.error(f"SQL-Fehler (insert_or_update): {e}\nQuery: {query}")
            raise

    def get_land_id(self, code='DE'):
        """Gibt die Land-ID fuer einen ISO-Code zurueck."""
        result = self.execute("SELECT id FROM laender WHERE code = %s", (code,))
        if result:
            return result[0]['id']
        return None

    def get_quelle_id(self, kuerzel, land_code='DE'):
        """Gibt die Datenquellen-ID fuer ein Kuerzel zurueck."""
        result = self.execute(
            """SELECT dq.id FROM datenquellen dq
               JOIN laender l ON dq.land_id = l.id
               WHERE dq.kuerzel = %s AND l.code = %s""",
            (kuerzel, land_code)
        )
        if result:
            return result[0]['id']
        return None


# Singleton-Instanz
db = Database()
