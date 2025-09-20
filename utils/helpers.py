import psycopg2
from typing import List, Dict
from config import config
from utils.logger import logger

def insert_employers(employers: List[Dict]) -> None:
    """Пакетная вставка данных работодателей"""
    query = """
        INSERT INTO employers (employer_id, name, url, open_vacancies)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (employer_id) DO NOTHING
    """
    try:
        conn = psycopg2.connect(**config())
        cur = conn.cursor()

        data = [
            (emp['id'], emp['name'], emp['alternate_url'], emp['open_vacancies'])
            for emp in employers
        ]
        cur.executemany(query, data)
        conn.commit()
        logger.info(f"Добавлено {len(employers)} работодателей")

    except psycopg2.Error as e:
        logger.error(f"Ошибка вставки работодателей: {e}")
        conn.rollback()
    finally:
        if conn: conn.close()

def batch_insert(table: str, data: list[dict], batch_size: int = 100):
    """Массовая вставка данных в указанную таблицу"""
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                columns = ', '.join(batch[0].keys())
                values = ', '.join([f"(%({k})s)" for k in batch[0].keys()])
                query = f"""
                    INSERT INTO {table} ({columns})
                    VALUES {values}
                    ON CONFLICT DO NOTHING
                """
                execute_batch(cursor, query, batch)

def insert_vacancies(vacancies: List[Dict]) -> None:
    """Пакетная вставка вакансий"""
    query = """
        INSERT INTO vacancies (employer_id, title, salary_from, 
            salary_to, currency, url)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    try:
        conn = psycopg2.connect(**config())
        cur = conn.cursor()

        data = []
        for vac in vacancies:
            salary = vac.get('salary')
            data.append((
                vac['employer']['id'],
                vac['name'],
                salary.get('from') if salary else None,
                salary.get('to') if salary else None,
                salary.get('currency') if salary else None,
                vac['alternate_url']
            ))

        cur.executemany(query, data)
        conn.commit()
        logger.info(f"Добавлено {len(vacancies)} вакансий")

    except psycopg2.Error as e:
        logger.error(f"Ошибка вставки вакансий: {e}")
        conn.rollback()
    finally:
        if conn: conn.close()