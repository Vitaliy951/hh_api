import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from typing import NoReturn, Dict, Any
from contextlib import closing
from utils.logger import logger

load_dotenv()


class DBCreator:
    """Класс для управления жизненным циклом БД с поддержкой транзакций и миграций"""

    def __init__(self) -> None:
        self.conn_params = self._load_and_validate_config()
        self.encoding = 'UTF8'
        self.locale = 'ru_RU.UTF-8'
        self.template = 'template0'

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Загрузка и строгая валидация конфигурации"""
        config = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }

        required = {
            'dbname': (str, 'Название БД'),
            'user': (str, 'Пользователь'),
            'password': (str, 'Пароль'),
            'host': (str, 'Хост'),
            'port': (lambda x: str(x).isdigit(), 'Порт должен быть числом')
        }

        for key, (check, msg) in required.items():
            if not config.get(key):
                raise ValueError(f"Отсутствует параметр: {key} ({msg})")
            if not check(config[key]):
                raise TypeError(f"Некорректный тип для {key}: {msg}")

        return config

    def _get_admin_connection(self):
        """Соединение с БД postgres для администрирования"""
        return psycopg2.connect(
            dbname='postgres',
            user=self.conn_params['user'],
            password=self.conn_params['password'],
            host=self.conn_params['host'],
            connect_timeout=5,
            options=f"-c statement_timeout={30 * 1000}"
        )

    def database_exists(self) -> bool:
        """Проверка существования БД"""
        query = sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}").format(
            sql.Literal(self.conn_params['dbname'])
        )

        try:
            with closing(self._get_admin_connection()) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    return bool(cur.fetchone())
        except psycopg2.Error as e:
            logger.error(f"Ошибка проверки БД: {str(e)}")
            return False

    def create_database(self) -> None:
        """Безопасное создание БД с шаблоном"""
        if self.database_exists():
            logger.info(f"БД {self.conn_params['dbname']} уже существует")
            return

        create_query = sql.SQL("""
            CREATE DATABASE {dbname} 
            WITH 
            ENCODING = {encoding}
            LC_COLLATE = {locale}
            LC_CTYPE = {locale}
            TEMPLATE = {template}
        """).format(
            dbname=sql.Identifier(self.conn_params['dbname']),
            encoding=sql.Literal(self.encoding),
            locale=sql.Literal(self.locale),
            template=sql.Identifier(self.template)
        )

        try:
            with closing(self._get_admin_connection()) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(create_query)
                    logger.info(f"Создана БД: {self.conn_params['dbname']}")
        except psycopg2.Error as e:
            logger.critical(f"Ошибка создания БД: {str(e)}")
            raise

    def _execute_migration(self, queries: list) -> None:
        """Выполнение миграций в транзакции"""
        with psycopg2.connect(**self.conn_params) as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    for query in queries:
                        cur.execute(query)
                    conn.commit()
                    logger.info("Миграции успешно применены")
            except Exception as e:
                conn.rollback()
                logger.error(f"Откат транзакции: {str(e)}")
                raise

    def create_tables(self) -> None:
        """Создание таблиц с расширенными ограничениями и индексами"""
        migrations = [
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS employers (
                    employer_id VARCHAR(20) PRIMARY KEY
                        CHECK (employer_id ~ '^\\d+$'),
                    name VARCHAR(100) NOT NULL,
                    url VARCHAR(200) 
                        CHECK (url ~ '^https?://[^\\s/$.?#].[^\\s]*$'),
                    open_vacancies INT
                        CHECK (open_vacancies >= 0),
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                COMMENT ON TABLE employers IS 'Таблица работодателей';
                COMMENT ON COLUMN employers.employer_id IS 'Идентификатор в HH';
            """),

            sql.SQL("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    vacancy_id VARCHAR(20) PRIMARY KEY
                        CHECK (vacancy_id ~ '^\\d+$'),
                    employer_id VARCHAR(20) NOT NULL
                        REFERENCES employers(employer_id)
                        ON DELETE CASCADE,
                    title VARCHAR(200) NOT NULL,
                    salary_from INT
                        CHECK (salary_from > 0),
                    salary_to INT
                        CHECK (salary_to > 0),
                    currency VARCHAR(3)
                        CHECK (currency ~ '^[A-Z]{3}$'),
                    url VARCHAR(200) NOT NULL
                        CHECK (url ~ '^https?://[^\\s/$.?#].[^\\s]*$'),
                    experience VARCHAR(50),
                    schedule VARCHAR(50),
                    posted_at TIMESTAMP,
                    archived BOOLEAN DEFAULT FALSE,
                    CONSTRAINT valid_salary_range 
                        CHECK (salary_from <= salary_to)
                );

                COMMENT ON TABLE vacancies IS 'Таблица вакансий';
                COMMENT ON COLUMN vacancies.currency IS 'Код валюты по ISO 4217';
            """),

            sql.SQL("""
                CREATE INDEX IF NOT EXISTS idx_employer_name 
                ON employers USING gin(name gin_trgm_ops);

                CREATE INDEX IF NOT EXISTS idx_vacancy_title 
                ON vacancies USING gin(title gin_trgm_ops);

                CREATE INDEX IF NOT EXISTS idx_vacancy_salary 
                ON vacancies (COALESCE(salary_from, salary_to));
            """)
        ]

        try:
            self._execute_migration([q.as_string(psycopg2.extensions.connection)
                                     for q in migrations])
        except psycopg2.Error as e:
            logger.error(f"Ошибка создания таблиц: {str(e)}")
            raise

    def drop_database(self) -> None:
        """Удаление БД (только для тестовых сред)"""
        if not self.database_exists():
            return

        terminate_sessions = sql.SQL("""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = {dbname}
        """).format(dbname=sql.Literal(self.conn_params['dbname']))

        drop_query = sql.SQL("DROP DATABASE {dbname}").format(
            dbname=sql.Identifier(self.conn_params['dbname'])
        )

        try:
            with closing(self._get_admin_connection()) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(terminate_sessions)
                    cur.execute(drop_query)
                    logger.warning(f"БД {self.conn_params['dbname']} удалена")
        except psycopg2.Error as e:
            logger.error(f"Ошибка удаления БД: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        creator = DBCreator()

        if not creator.database_exists():
            creator.create_database()

        creator.create_tables()
        logger.success("Инициализация БД завершена успешно")

    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        raise
