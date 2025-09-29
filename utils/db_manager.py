import sqlite3
from typing import List, Dict
from contextlib import contextmanager
from utils.logger import logger

class DBManager:
    def __init__(self, db_path: str = 'vacancies.db'):
        self.db_path = db_path

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database error: {str(e)}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                return [dict(row) for row in cursor.fetchall()]
            except sqlite3.Error as e:
                logger.error(f"Query failed: {str(e)}")
                raise

    def get_companies_stats(self) -> List[Dict]:
        query = '''
            SELECT 
                e.id AS employer_id,
                e.name AS company,
                COUNT(v.id) AS total_vacancies,
                AVG(v.salary) AS avg_salary
            FROM employers e
            LEFT JOIN vacancies v ON e.id = v.employer_id
            GROUP BY e.id
            HAVING total_vacancies > 0
            ORDER BY total_vacancies DESC
        '''
        return self.execute_query(query)

    def get_vacancies(self, min_salary: int = None) -> List[Dict]:
        query = '''
            SELECT 
                e.name AS company,
                v.title,
                v.salary,
                v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.id
            WHERE v.salary >= ?
            ORDER BY v.salary DESC
        '''
        return self.execute_query(query, (min_salary or 0,))
