import psycopg2
from typing import List, Optional
from contextlib import contextmanager


class DBManager:
    """Менеджер для работы с базой данных PostgreSQL"""

    def __init__(self, dbname: str, user: str, password: str, host: str):
        self.conn_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host
        }

    @contextmanager
    def _get_cursor(self):
        conn = psycopg2.connect(**self.conn_params)
        try:
            with conn.cursor() as cursor:
                yield cursor
            conn.commit()
        finally:
            conn.close()

    def get_companies_and_vacancies_count(self) -> List[tuple]:
        """Получить список компаний с количеством вакансий"""
        query = """
            SELECT e.name, COUNT(v.id) 
            FROM employers e 
            LEFT JOIN vacancies v ON e.id = v.employer_id 
            GROUP BY e.id
        """
        with self._get_cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def get_all_vacancies(self) -> List[tuple]:
        """Получить все вакансии с деталями"""
        query = """
            SELECT e.name, v.name, v.salary_from, v.salary_to, v.url 
            FROM vacancies v 
            JOIN employers e ON v.employer_id = e.id
        """
        with self._get_cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def get_avg_salary(self) -> float:
        """Рассчитать среднюю зарплату"""
        query = """
            SELECT AVG((salary_from + salary_to)/2) 
            FROM vacancies 
            WHERE salary_from IS NOT NULL 
            AND salary_to IS NOT NULL
        """
        with self._get_cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]

    def get_vacancies_with_higher_salary(self) -> List[tuple]:
        """Вакансии с зарплатой выше средней"""
        avg_salary = self.get_avg_salary()
        query = """
            SELECT * FROM vacancies 
            WHERE (salary_from + salary_to)/2 > %s
        """
        with self._get_cursor() as cur:
            cur.execute(query, (avg_salary,))
            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[tuple]:
        """Поиск вакансий по ключевому слову"""
        query = """
            SELECT * FROM vacancies 
            WHERE name ILIKE %s
        """
        with self._get_cursor() as cur:
            cur.execute(query, (f'%{keyword}%',))
            return cur.fetchall()
