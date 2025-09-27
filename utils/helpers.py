from typing import List, Dict, Any, Optional
from psycopg2.extras import execute_batch
from database.db_manager import DBManager
from utils.logger import logger
from exceptions import DataValidationError


class DBHelper:
    """Класс для пакетных операций с базой данных с улучшенной обработкой ошибок"""

    def __init__(self, db_manager: DBManager):
        """Инициализация с валидацией менеджера БД"""
        if not isinstance(db_manager, DBManager):
            raise TypeError("Неверный тип DBManager")
        self.db = db_manager

    def insert_employers(self, employers: List[Dict[str, Any]]) -> int:
        """
        Пакетная вставка работодателей с улучшенной валидацией

        Args:
            employers: Список словарей с обязательными полями:
                - id (str)
                - name (str)
                - alternate_url (str)
                - open_vacancies (int)

        Returns:
            Количество успешно вставленных записей
        """
        if not employers:
            logger.warning("Пустой список работодателей для вставки")
            return 0

        query = """
            INSERT INTO employers (
                employer_id, name, url, open_vacancies
            ) VALUES (%(id)s, %(name)s, %(url)s, %(vacancies)s)
            ON CONFLICT (employer_id) DO UPDATE SET
                name = EXCLUDED.name,
                url = EXCLUDED.url,
                open_vacancies = EXCLUDED.open_vacancies
        """

        processed_data = []
        for idx, emp in enumerate(employers):
            try:
                processed_data.append({
                    'id': self._validate_id(emp['id']),
                    'name': self._truncate_string(emp['name'], 100),
                    'url': self._validate_url(emp.get('alternate_url', '')),
                    'vacancies': self._validate_int(emp.get('open_vacancies', 0))
                })
            except (KeyError, ValueError) as e:
                logger.error(f"Ошибка валидации работодателя #{idx}: {str(e)}")
                continue

        return self._batch_execute(query, processed_data, "работодателей")

    def insert_vacancies(self, vacancies: List[Dict[str, Any]]) -> int:
        """
        Пакетная вставка вакансий с нормализацией зарплаты

        Args:
            vacancies: Список словарей с обязательными полями:
                - id (str)
                - name (str)
                - employer (dict с id)
                - salary (dict или None)
                - alternate_url (str)

        Returns:
            Количество успешно вставленных записей
        """
        if not vacancies:
            logger.warning("Пустой список вакансий для вставки")
            return 0

        query = """
            INSERT INTO vacancies (
                vacancy_id, employer_id, title, 
                salary_from, salary_to, currency, url
            ) VALUES (
                %(id)s, %(employer_id)s, %(title)s,
                %(salary_from)s, %(salary_to)s, %(currency)s, %(url)s
            )
            ON CONFLICT (vacancy_id) DO UPDATE SET
                title = EXCLUDED.title,
                salary_from = EXCLUDED.salary_from,
                salary_to = EXCLUDED.salary_to,
                currency = EXCLUDED.currency,
                url = EXCLUDED.url
        """

        processed_data = []
        for idx, vac in enumerate(vacancies):
            try:
                salary = vac.get('salary') or {}
                processed_data.append({
                    'id': self._validate_id(vac['id']),
                    'employer_id': self._validate_id(vac['employer']['id']),
                    'title': self._truncate_string(vac['name'], 200),
                    'salary_from': self._validate_salary(salary.get('from')),
                    'salary_to': self._validate_salary(salary.get('to')),
                    'currency': self._normalize_currency(salary.get('currency')),
                    'url': self._validate_url(vac.get('alternate_url', ''))
                })
            except (KeyError, ValueError) as e:
                logger.error(f"Ошибка валидации вакансии #{idx}: {str(e)}")
                continue

        return self._batch_execute(query, processed_data, "вакансий")

    def _batch_execute(
            self,
            query: str,
            data: List[dict],
            entity_name: str,
            batch_size: int = 500
    ) -> int:
        """
        Универсальный метод для пакетных операций с транзакциями

        Args:
            query: SQL-запрос с именованными параметрами
            data: Список словарей с параметрами
            entity_name: Название сущности для логов
            batch_size: Размер пакета

        Returns:
            Количество успешно обработанных записей
        """
        if not data:
            logger.warning(f"Нет данных для вставки {entity_name}")
            return 0

        try:
            with self.db._get_cursor() as cur:  # Используем контекстный менеджер
                total = 0
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    execute_batch(cur, query, batch)
                    total += cur.rowcount
                logger.info(f"Успешно обработано {total} {entity_name}")
                return total
        except Exception as e:
            logger.error(f"Ошибка пакетной вставки {entity_name}: {str(e)}")
            raise DataValidationError(f"Ошибка вставки данных: {str(e)}") from e

    def clear_tables(self, tables: List[str] = None) -> None:
        """
        Безопасная очистка таблиц с проверкой существования

        Args:
            tables: Список таблиц для очистки (по умолчанию vacancies, employers)
        """
        tables = tables or ['vacancies', 'employers']
        valid_tables = {'vacancies', 'employers'}

        try:
            with self.db._get_cursor() as cur:
                for table in tables:
                    if table not in valid_tables:
                        raise ValueError(f"Недопустимая таблица: {table}")
                    cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                logger.info(f"Очищены таблицы: {', '.join(tables)}")
        except Exception as e:
            logger.error(f"Ошибка очистки таблиц: {str(e)}")
            raise

    # Вспомогательные методы валидации
    @staticmethod
    def _validate_id(value: Any) -> str:
        """Валидация идентификатора"""
        str_id = str(value).strip()
        if not str_id.isdigit():
            raise ValueError("ID должен содержать только цифры")
        return str_id

    @staticmethod
    def _truncate_string(value: str, max_length: int) -> str:
        """Обрезка строки с сохранением целостности UTF-8"""
        return value.encode('utf-8')[:max_length].decode('utf-8', 'ignore').strip()

    @staticmethod
    def _validate_int(value: Any) -> int:
        """Приведение к целому числу"""
        try:
            return int(value)
        except (ValueError, TypeError):
            raise ValueError("Некорректное целое число")

    @staticmethod
    def _validate_salary(value: Any) -> Optional[int]:
        """Валидация суммы зарплаты"""
        if value is None:
            return None
        try:
            num = int(value)
            if num < 0:
                raise ValueError("Зарплата не может быть отрицательной")
            return num
        except (ValueError, TypeError):
            raise ValueError("Некорректное значение зарплаты")

    @staticmethod
    def _normalize_currency(currency: str) -> str:
        """Нормализация валюты"""
        if not currency:
            return 'RUR'
        return currency.strip().upper()[:3]

    @staticmethod
    def _validate_url(url: str) -> str:
        """Базовая проверка URL"""
        if not url.startswith(('http://', 'https://')):
            raise ValueError("Некорректный URL-адрес")
        return url[:200]


if __name__ == "__main__":
    # Пример использования с тестовыми данными
    from database.db_manager import DBManager
    from config import config

    test_data = [{
        'id': '12345',
        'name': 'Тестовая компания ' * 10,
        'alternate_url': 'https://test.company.ru',
        'open_vacancies': 'invalid'
    }]

    try:
        manager = DBManager(config.db_config)
        helper = DBHelper(manager)
        helper.insert_employers(test_data)
    except Exception as e:
        logger.error(f"Тест завершился ошибкой: {str(e)}")