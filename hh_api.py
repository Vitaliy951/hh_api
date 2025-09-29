import requests
import psycopg2
from typing import List, Dict
from config import DB_CONFIG
from database.models import create_tables

# Список ID компаний с hh.ru (пример)
EMPLOYER_IDS = [
    '15478',  # VK
    '1740',  # Яндекс
    '3529',  # Сбер
    '78638',  # Тинькофф
    '87021',  # Wildberries
    '2180',  # OZON
    '3776',  # МТС
    '39305',  # Газпром нефть
    '64174',  # 2ГИС
    '1122462'  # Ростелеком
]


def get_employer_data(employer_id: str) -> Dict:
    """Получение данных о компании"""
    url = f'https://api.hh.ru/employers/{employer_id}'
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None


def get_vacancies(employer_id: str) -> List[Dict]:
    """Получение вакансий компании"""
    url = f'https://api.hh.ru/vacancies?employer_id={employer_id}&per_page=100'
    response = requests.get(url)
    return response.json().get('items', []) if response.status_code == 200 else []


def save_to_database(employers_data: List[Dict], vacancies_data: List[Dict]):
    """Сохранение данных в PostgreSQL"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Вставка работодателей
            for employer in employers_data:
                cur.execute(
                    "INSERT INTO employers (name, hh_id) VALUES (%s, %s) RETURNING id",
                    (employer['name'], employer['id'])
                )
                employer_db_id = cur.fetchone()[0]

                # Вставка вакансий
                for vacancy in filter(lambda v: v['employer']['id'] == employer['id'], vacancies_data):
                    salary = vacancy.get('salary')
                    cur.execute(
                        """INSERT INTO vacancies 
                           (title, salary_from, salary_to, url, employer_id)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (
                            vacancy['name'],
                            salary['from'] if salary else None,
                            salary['to'] if salary else None,
                            vacancy['alternate_url'],
                            employer_db_id
                        )
                    )
            conn.commit()
    finally:
        conn.close()


def load_data_to_db():
    """Основная функция загрузки данных"""
    create_tables()

    all_employers = []
    all_vacancies = []

    for employer_id in EMPLOYER_IDS:
        if employer_data := get_employer_data(employer_id):
            all_employers.append(employer_data)
            all_vacancies.extend(get_vacancies(employer_id))

    save_to_database(all_employers, all_vacancies)
    print(f"Загружено: {len(all_employers)} компаний и {len(all_vacancies)} вакансий")