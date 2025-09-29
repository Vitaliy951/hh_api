import psycopg2
from psycopg2 import sql, errors
from typing import Optional
from config import DB_CONFIG
from utils.logger import logger


class DBCreator:
    def __init__(self):
        self.conn: Optional[psycopg2.extensions.connection] = None
        self._create_database()
        self._connect()

    def _create_database(self) -> None:
        """Создание БД если отсутствует"""
        admin_conn = None
        try:
            admin_conn = psycopg2.connect(
                dbname='postgres',
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                host=DB_CONFIG['host']
            )
            admin_conn.autocommit = True

            with admin_conn.cursor() as cursor:
                # Безопасное экранирование имени БД
                query = sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(DB_CONFIG['dbname'])
                )
                cursor.execute(query)
                logger.info(f"БД {DB_CONFIG['dbname']} успешно создана")

        except errors.DuplicateDatabase:
            logger.debug(f"БД {DB_CONFIG['dbname']} уже существует")
        except psycopg2.OperationalError as e:
            logger.critical(f"Критическая ошибка подключения: {str(e)}")
            raise
        finally:
            if admin_conn and not admin_conn.closed:
                admin_conn.close()

    def _connect(self) -> None:
        """Установка основного соединения"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False  # Ручное управление транзакциями
            logger.debug("Успешное подключение к БД")
        except psycopg2.Error as e:
            logger.error(f"Ошибка подключения: {str(e)}")
            raise

    def create_tables(self) -> None:
        """Инициализация схемы данных"""
        table_definitions = {
            'employers': """
                CREATE TABLE IF NOT EXISTS employers (
                    id SERIAL PRIMARY KEY,
                    hh_id VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    area VARCHAR(100),
                    open_vacancies INT
                )
            """,
            'vacancies': """
                CREATE TABLE IF NOT EXISTS vacancies (
                    id SERIAL PRIMARY KEY,
                    hh_id VARCHAR(20) UNIQUE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    salary_from INT,
                    salary_to INT,
                    currency VARCHAR(3),
                    experience VARCHAR(50),
                    schedule VARCHAR(50),
                    employer_id INT REFERENCES employers(id) ON DELETE CASCADE,
                    url VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }

        try:
            with self.conn.cursor() as cursor:
                for table, ddl in table_definitions.items():
                    try:
                        cursor.execute(ddl)
                        logger.debug(f"Таблица {table} создана/проверена")
                    except errors.DuplicateTable:
                        logger.debug(f"Таблица {table} уже существует")
                self.conn.commit()
        except psycopg2.DatabaseError as e:
            logger.error(f"Ошибка при создании таблиц: {str(e)}")
            self.conn.rollback()
            raise

    def check_connection(self) -> bool:
        """Проверка работоспособности соединения"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except psycopg2.InterfaceError:
            return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Соединение закрыто через контекстный менеджер")

    def __del__(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.debug("Соединение закрыто в деструкторе")