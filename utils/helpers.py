import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any
from config import config
from utils.logger import logger


class DBHelper:
    def __init__(self):
        self.conn_params = config()
        self._validate_config()

    def _validate_config(self) -> None:
        """Проверка валидности конфигурации подключения"""
        required = ['dbname', 'user', 'password', 'host', 'port']
        if not all(self.conn_params.get(key) for key in required):
            missing = [key for key in required if not self.conn_params.get(key)]
            raise ValueError(f"Неполная конфигурация БД: {', '.join(missing)}")

    def _connect(self) -> psycopg2.extensions.connection:
        """Установка соединения с таймаутами"""
        return psycopg2.connect(
            **self.conn_params,
            connect_timeout=5,
            options="-c statement_timeout=30000"
        )

    def insert_employers(self, employers: List[Dict[str, Any]]) -> int:
        """
        Пакетная вставка данных работодателей

        Args:
            employers: Список словарей с данными работодателей

        Returns:
            Количество успешно добавленных записей
        """
        query = """
            INSERT INTO employers (
                employer_id, 
                name, 
                url, 
                open_vacancies
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (employer_id) DO UPDATE SET
                name = EXCLUDED.name,
                url = EXCLUDED.url,
                open_vacancies = EXCLUDED.open_vacancies
        """
        data = [
            (
                emp['id'],
                emp['name'][:100],  # Ограничение длины по схеме БД
                emp.get('alternate_url', '')[:200],
                emp.get('open_vacancies', 0)
            )
            for emp in employers
        ]

        return self._batch_execute(query, data, "работодателей")

    def insert_vacancies(self, vacancies: List[Dict[str, Any]]) -> int:
        """
        Пакетная вставка вакансий с обработкой зарплаты

        Args:
            vacancies: Список вакансий в формате API HH

        Returns:
            Количество успешно добавленных вакансий
        """
        query = """
            INSERT INTO vacancies (
                vacancy_id,
                employer_id, 
                title, 
                salary_from, 
                salary_to, 
                currency, 
                url
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
            employer = vac.get('employer') or {}

            data.append((
                vac['id'],
                employer.get('id'),
                vac['name'][:200],  # Ограничение по схеме БД
                salary.get('from'),
                salary.get('to'),
                salary.get('currency', 'RUR')[:3],  # ISO currency code
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
        """Универсальный метод пакетного выполнения"""
        inserted = 0
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    for offset in range(0, len(data), batch_size):
                        batch = data[offset:offset + batch_size]
                        execute_batch(cur, query, batch)
                        inserted += cur.rowcount
                    conn.commit()

                    logger.info(
                        f"Успешно добавлено {inserted} {entity_name} "
                        f"(дубликатов: {len(data) - inserted})"
                    )
                    return inserted

        except psycopg2.Error as e:
            logger.error(f"Ошибка вставки {entity_name}: {e}")
            conn.rollback()
            return 0

    def clear_tables(self, tables: List[str] = None) -> None:
        """Очистка таблиц с проверкой существования"""
        tables = tables or ['vacancies', 'employers']
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    for table in tables:
                        cur.execute(f"""
                            TRUNCATE TABLE {table} 
                            RESTART IDENTITY 
                            CASCADE
                        """)
                    conn.commit()
                    logger.info(f"Таблицы {', '.join(tables)} очищены")

        except psycopg2.Error as e:
            logger.error(f"Ошибка очистки таблиц: {e}")
            raise


if __name__ == "__main__":
    helper = DBHelper()
    # Пример использования
    sample_employers = [{
        'id': '123',
        'name': 'Test Company',
        'alternate_url': 'http://example.com',
        'open_vacancies': 5
    }]
    helper.insert_employers(sample_employers)