from typing import List, Dict, Any
from psycopg2.extras import execute_batch
from database.db_manager import DBManager
from utils.logger import logger


class DBHelper:
    def __init__(self, db_manager: DBManager):
        """Инициализация хелпера с менеджером БД"""
        self.db = db_manager

    def insert_employers(self, employers: List[Dict[str, Any]]) -> int:
        """Пакетная вставка работодателей с обработкой конфликтов"""
        query = """
            INSERT INTO employers (
                employer_id, name, url, open_vacancies
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (employer_id) DO UPDATE SET
                name = EXCLUDED.name,
                url = EXCLUDED.url,
                open_vacancies = EXCLUDED.open_vacancies
        """
        data = [
            (
                emp['id'],
                emp['name'][:100],
                emp.get('alternate_url', '')[:200],
                emp.get('open_vacancies', 0)
            )
            for emp in employers
        ]
        return self._batch_execute(query, data, "работодателей")

    def insert_vacancies(self, vacancies: List[Dict[str, Any]]) -> int:
        """Пакетная вставка вакансий с нормализацией зарплаты"""
        query = """
            INSERT INTO vacancies (
                vacancy_id, employer_id, title, 
                salary_from, salary_to, currency, url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vacancy_id) DO UPDATE SET
                title = EXCLUDED.title,
                salary_from = EXCLUDED.salary_from,
                salary_to = EXCLUDED.salary_to,
                currency = EXCLUDED.currency,
                url = EXCLUDED.url
        """
        data = []
        for vac in vacancies:
            salary = vac.get('salary') or {}
            data.append((
                vac['id'],
                vac.get('employer', {}).get('id'),
                vac['name'][:200],
                salary.get('from'),
                salary.get('to'),
                salary.get('currency', 'RUR')[:3],
                vac.get('alternate_url', '')[:200]
            ))
        return self._batch_execute(query, data, "вакансий")

    def _batch_execute(
            self,
            query: str,
            data: List[tuple],
            entity_name: str,
            batch_size: int = 500
    ) -> int:
        """Универсальный метод для пакетных операций"""
        inserted = 0
        try:
            with self.db.conn.cursor() as cur:
                for offset in range(0, len(data), batch_size):
                    execute_batch(cur, query, data[offset:offset + batch_size])
                    inserted += cur.rowcount
                self.db.conn.commit()
                logger.info(f"Добавлено {inserted} {entity_name} (дубликатов: {len(data) - inserted})")
                return inserted
        except Exception as e:
            logger.error(f"Ошибка вставки {entity_name}: {str(e)}")
            self.db.conn.rollback()
            return 0

    def clear_tables(self, tables: List[str] = None) -> None:
        """Безопасная очистка указанных таблиц"""
        tables = tables or ['vacancies', 'employers']
        try:
            with self.db.conn.cursor() as cur:
                for table in tables:
                    cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                self.db.conn.commit()
                logger.info(f"Очищены таблицы: {', '.join(tables)}")
        except Exception as e:
            logger.error(f"Ошибка очистки таблиц: {str(e)}")
            self.db.conn.rollback()
            raise


if __name__ == "__main__":
    # Пример тестирования
    from config import config
    from database.db_manager import DBManager

    db_manager = DBManager(config.db_config)
    helper = DBHelper(db_manager)

    # Тестовые данные
    test_employers = [{
        'id': '12345',
        'name': 'Тестовая компания',
        'alternate_url': 'https://test.company.ru',
        'open_vacancies': 3
    }]

    helper.insert_employers(test_employers)
