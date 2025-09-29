import sqlite3
from utils.logger import logger

class DBCreator:
    def __init__(self, db_path: str = 'vacancies.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)

    def _execute_script(self, script: str):
        try:
            cursor = self.conn.cursor()
            cursor.executescript(script)
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"DB creation error: {str(e)}")
            raise

    def create_tables(self):
        script = '''
            CREATE TABLE IF NOT EXISTS employers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT
            );

            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                title TEXT,
                salary INTEGER,
                url TEXT,
                employer_id INTEGER,
                FOREIGN KEY(employer_id) REFERENCES employers(id)
            );
        '''
        self._execute_script(script)
        logger.info("Database schema created")

    def __del__(self):
        if self.conn:
            self.conn.close()
