import psycopg2
from typing import List, Dict, Optional
from config import config
from utils.logger import logger

class DBManager:
    def __init__(self):
        self.params = config()

    def _connect(self):
        return psycopg2.connect(**self.params)

    def get_companies_and_vacancies_count(self) -> List[Dict]:
        """Возвращает список компаний с количеством вакансий"""
        query = """
            SELECT e.name, COUNT(v.vacancy_id) 
            FROM employers e
            LEFT JOIN vacancies v ON e.employer_id = v.employer_id
            GROUP BY e.name
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    return [{'company': row[0], 'count': row[1]} for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения данных: {e}")
            return []

    def get_avg_salary(self) -> float:
        """Рассчитывает среднюю зарплату по всем вакансиям"""
        query = """
            SELECT AVG((salary_from + salary_to)/2) 
            FROM vacancies 
            WHERE salary_from IS NOT NULL 
              AND salary_to IS NOT NULL
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    return round(cur.fetchone()[0], 2)
        except Exception as e:
            logger.error(f"Ошибка расчета зарплаты: {e}")
            return 0.0

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict]:
        """Ищет вакансии по ключевому слову в названии"""
        query = """
            SELECT title, url 
            FROM vacancies 
            WHERE title ILIKE %s
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, ('%' + keyword + '%',))
                    return [{'title': row[0], 'url': row[1]} for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка поиска вакансий: {e}")
            return []