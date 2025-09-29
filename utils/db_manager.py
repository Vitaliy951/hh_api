import psycopg2
from typing import List, Tuple, Optional
from config import DB_CONFIG


class DBManager:
    """Класс для управления данными в PostgreSQL"""

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """Возвращает ТОП-10 компаний по количеству вакансий"""
        query = """
            SELECT e.name, COUNT(v.id) 
            FROM employers e
            LEFT JOIN vacancies v ON e.id = v.employer_id
            GROUP BY e.name
            ORDER BY COUNT(v.id) DESC
            LIMIT 10
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_all_vacancies(self) -> List[str]:
        """Возвращает вакансии в человекочитаемом формате"""
        query = """
            SELECT e.name, v.title, 
                   COALESCE(v.salary_from, v.salary_to, 'Не указана') as salary,
                   v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.id
        """
        self.cursor.execute(query)
        return [f"{row[0]} | {row[1]} | Зарплата: {row[2]} | {row[3]}" for row in self.cursor.fetchall()]

    def get_avg_salary(self) -> float:
        """Рассчитывает среднюю зарплату"""
        self.cursor.execute("SELECT AVG((salary_from + salary_to)/2) FROM vacancies")
        return round(self.cursor.fetchone()[0], 2)

    def get_vacancies_with_higher_salary(self) -> List[Tuple]:
        """Вакансии с зарплатой выше средней"""
        avg = self.get_avg_salary()
        query = f"""
            SELECT title, salary_from, salary_to, url 
            FROM vacancies 
            WHERE (salary_from + salary_to)/2 > {avg}
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple]:
        """Поиск вакансий по ключевым словам"""
        query = f"""
            SELECT title, salary_from, salary_to, url 
            FROM vacancies 
            WHERE title ILIKE '%{keyword}%'
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def __del__(self):
        self.cursor.close()
        self.conn.close()