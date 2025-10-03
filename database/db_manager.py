import psycopg2
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from config import DB_CONFIG
from utils.logger import logger


class DBManager:
    def __init__(self):
        self.conn = None  # Инициализация в __enter__

    def __enter__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.debug("Соединение с БД закрыто")

    @contextmanager
    def _get_cursor(self):
        if not self.conn or self.conn.closed:
            raise RuntimeError("Нет активного соединения с БД")

        cursor = self.conn.cursor()
        try:
            yield cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка БД: {str(e)}")
            raise
        finally:
            cursor.close()

    def has_data(self) -> bool:
        with self._get_cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM employers LIMIT 1)")
            return cur.fetchone()[0]

    def insert_employer(self, hh_id: str, name: str,
                      description: Optional[str] = None,
                      area: Optional[str] = None,
                      open_vacancies: Optional[int] = None):
        with self._get_cursor() as cur:
            cur.execute("""
                INSERT INTO employers (hh_id, name, description, area, open_vacancies)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hh_id) DO NOTHING
            """, (hh_id, name, description, area, open_vacancies))

    def insert_vacancy(self, hh_id: str, title: str,
                     salary_from: Optional[int] = None,
                     salary_to: Optional[int] = None,
                     currency: Optional[str] = None,
                     experience: Optional[str] = None,
                     schedule: Optional[str] = None,
                     employer_hh_id: Optional[str] = None,
                     url: Optional[str] = None):
        with self._get_cursor() as cur:
            # Получаем employer_id по hh_id
            cur.execute("SELECT id FROM employers WHERE hh_id = %s", (employer_hh_id,))
            employer_id = cur.fetchone()[0] if cur.rowcount > 0 else None

            if employer_id:
                cur.execute("""
                    INSERT INTO vacancies 
                    (hh_id, title, salary_from, salary_to, currency, 
                     experience, schedule, employer_id, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hh_id) DO NOTHING
                """, (hh_id, title, salary_from, salary_to, currency,
                      experience, schedule, employer_id, url))

    def get_all_vacancies(self) -> List[str]:
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT e.name, v.title, 
                    COALESCE(
                        CONCAT(
                            NULLIF(v.salary_from::text, ''),
                            CASE WHEN v.salary_to IS NOT NULL THEN '-' || v.salary_to::text ELSE '' END,
                            ' ', v.currency
                        ), 
                        'Не указана'
                    ) as salary,
                    v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
            """)
            return [f"{row[0]} | {row[1]} | Зарплата: {row[2]} | {row[3]}" for row in cur.fetchall()]

    def get_companies_and_vacancies_count(self) -> List[tuple]:
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT name, open_vacancies 
                FROM employers 
                ORDER BY open_vacancies DESC 
                LIMIT 10
            """)
            return cur.fetchall()

    def get_avg_salary(self) -> float:
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT AVG((salary_from + salary_to)/2) 
                FROM vacancies 
                WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
            """)
            return round(cur.fetchone()[0], 2)

    def get_vacancies_with_higher_salary(self) -> List[tuple]:
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT title, salary_from, salary_to, url 
                FROM vacancies 
                WHERE (salary_from + salary_to)/2 > (SELECT AVG((salary_from + salary_to)/2) FROM vacancies)
            """)
            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[tuple]:
        with self._get_cursor() as cur:
            cur.execute("""
                SELECT title, salary_from, salary_to, url 
                FROM vacancies 
                WHERE title ILIKE %s
            """, (f'%{keyword}%',))
            return cur.fetchall()

    def clear_tables(self):
        with self._get_cursor() as cur:
            cur.execute("TRUNCATE TABLE vacancies, employers RESTART IDENTITY CASCADE")

    def __del__(self):
        if self.conn and not self.conn.closed:
            self.conn.close()