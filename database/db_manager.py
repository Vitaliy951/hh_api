import psycopg2
from typing import List, Dict, Optional, Callable, Any
from config import config
from utils.logger import logger

class DBManager:
    def __init__(self) -> None:
        self.params = config()
        self._validate_config()

    def _validate_config(self) -> None:
        """Проверка валидности конфигурации подключения"""
        required = ['dbname', 'user', 'password', 'host', 'port']
        if not all(self.params.get(key) for key in required):
            missing = [key for key in required if not self.params.get(key)]
            raise ValueError(f"Неполная конфигурация БД: {', '.join(missing)}")

    def _connect(self) -> psycopg2.extensions.connection:
        """Установка соединения с таймаутами"""
        return psycopg2.connect(
            **self.params,
            connect_timeout=5,
            options="-c statement_timeout=30000"
        )

    def _execute_query(
        self,
        query: str,
        params: tuple = (),
        mapper: Optional[Callable[[Any], Dict]] = None
    ) -> List[Dict]:
        """Универсальный метод выполнения запросов"""
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if mapper:
                        return [mapper(row) for row in cur.fetchall()]
                    return []
        except psycopg2.Error as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            return []

    def get_companies_and_vacancies_count(self) -> List[Dict]:
        """Получение списка компаний с количеством вакансий"""
        query = """
            SELECT 
                e.employer_id,
                e.name AS company_name, 
                COUNT(v.vacancy_id) AS vacancies_count
            FROM employers e
            LEFT JOIN vacancies v USING(employer_id)
            GROUP BY e.employer_id
            ORDER BY vacancies_count DESC
        """
        return self._execute_query(query, mapper=lambda row: {
            'employer_id': row[0],
            'company': row[1],
            'vacancies_count': row[2]
        })

    def get_all_vacancies(self) -> List[Dict]:
        """Получение всех вакансий с детализацией"""
        query = """
            SELECT 
                e.name AS company,
                v.title,
                v.salary_from,
                v.salary_to,
                v.currency,
                v.url
            FROM vacancies v
            JOIN employers e USING(employer_id)
            ORDER BY COALESCE(v.salary_from, v.salary_to) DESC
        """
        return self._execute_query(query, mapper=lambda row: {
            'company': row[0],
            'title': row[1],
            'salary': f"{row[2] or '?'} - {row[3] or '?'} {row[4]}",
            'url': row[5]
        })

    def get_avg_salary(self) -> float:
        """Расчет средней зарплаты с учетом частичных данных"""
        query = """
            SELECT AVG(
                CASE 
                    WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                        THEN (salary_from + salary_to) / 2
                    ELSE COALESCE(salary_from, salary_to)
                END
            )::numeric(10,2)
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
        """
        result = self._execute_query(query)
        return result[0]['avg'] if result else 0.0

    def get_vacancies_with_higher_salary(self) -> List[Dict]:
        """Вакансии с зарплатой выше средней"""
        query = """
            WITH avg_salary AS (
                SELECT AVG(
                    CASE 
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL 
                            THEN (salary_from + salary_to) / 2
                        ELSE COALESCE(salary_from, salary_to)
                    END
                ) AS avg
                FROM vacancies
            )
            SELECT 
                v.title,
                v.url,
                COALESCE((v.salary_from + v.salary_to)/2, 
                        v.salary_from, v.salary_to) AS salary
            FROM vacancies v, avg_salary
            WHERE COALESCE((v.salary_from + v.salary_to)/2, 
                          v.salary_from, v.salary_to) > avg_salary.avg
            ORDER BY salary DESC
        """
        return self._execute_query(query, mapper=lambda row: {
            'title': row[0],
            'url': row[1],
            'salary': int(row[2])
        })

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict]:
        """Поиск вакансий по ключевым словам в названии"""
        query = """
            SELECT 
                title,
                url,
                COALESCE((salary_from + salary_to)/2, 
                        salary_from, salary_to) AS salary
            FROM vacancies 
            WHERE title ILIKE %s
            ORDER BY salary DESC NULLS LAST
        """
        return self._execute_query(query, (f'%{keyword}%',),
            mapper=lambda row: {
                'title': row[0],
                'url': row[1],
                'salary': row[2] or 'Не указана'
            })

    def get_vacancies_by_employer(self, employer_id: str) -> List[Dict]:
        """Получение вакансий конкретного работодателя"""
        query = """
            SELECT 
                title,
                salary_from,
                salary_to,
                currency,
                url
            FROM vacancies
            WHERE employer_id = %s
            ORDER BY COALESCE(salary_from, salary_to) DESC
        """
        return self._execute_query(query, (employer_id,),
            mapper=lambda row: {
                'title': row[0],
                'salary': f"{row[1] or '?'} - {row[2] or '?'} {row[3]}",
                'url': row[4]
            })

if __name__ == "__main__":
    manager = DBManager()
    print("Пример использования:")
    print("Компании и количество вакансий:", manager.get_companies_and_vacancies_count()[:2])
    print("Средняя зарплата:", manager.get_avg_salary())