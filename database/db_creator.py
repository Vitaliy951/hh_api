import psycopg2
from psycopg2 import sql, errors
from typing import Optional, Dict
from config import DB_CONFIG
from utils.logger import logger


class DBCreator:
    def __init__(self):
        self.conn: Optional[psycopg2.extensions.connection] = None
        logger.debug(f"Инициализация с конфигом: {self._masked_config()}")

    def _masked_config(self) -> Dict:
        """Маскировка конфиденциальных данных в логах"""
        return {k: '***' if k == 'password' else v for k, v in DB_CONFIG.items()}

    def __enter__(self):
        try:
            self._validate_config()
            self._create_database()
            self._connect()
            return self
        except Exception as e:
            logger.critical(f"Ошибка инициализации: {str(e)}")
            self._safe_close()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._safe_close()
        logger.info("Контекстный менеджер завершил работу")

    def _safe_close(self) -> None:
        """Безопасное закрытие соединения с обработкой исключений"""
        if self.conn and not self.conn.closed:
            try:
                self.conn.close()
                logger.debug("Соединение закрыто")
            except psycopg2.InterfaceError as e:
                logger.warning(f"Ошибка при закрытии: {str(e)}")
            finally:
                self.conn = None

    def _validate_config(self) -> None:
        """Проверка валидности конфигурации"""
        required = {'dbname', 'user', 'password', 'host', 'port'}
        if missing := required - DB_CONFIG.keys():
            logger.critical(f"Отсутствуют параметры: {missing}")
            raise ValueError(f"Неполная конфигурация БД. Отсутствуют: {missing}")

        if not isinstance(DB_CONFIG['port'], int):
            raise TypeError("Порт должен быть целым числом")

        if not DB_CONFIG.get('password'):
            raise ValueError("Пароль не указан в конфигурации")

    def _create_database(self) -> None:
        """Безопасное создание БД с экранированием имен"""
        try:
            temp_config = {
                'user': DB_CONFIG['user'],
                'password': DB_CONFIG['password'],
                'host': DB_CONFIG['host'],
                'port': DB_CONFIG['port']
            }

            with psycopg2.connect(dbname='postgres', **temp_config) as temp_conn:
                temp_conn.autocommit = True
                with temp_conn.cursor() as cur:
                    dbname_escaped = sql.Identifier(DB_CONFIG['dbname'])
                    cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(dbname_escaped))
                    cur.execute(sql.SQL("CREATE DATABASE {}").format(dbname_escaped))
                    logger.info(f"База {DB_CONFIG['dbname']} пересоздана")
        except KeyError as e:
            logger.critical(f"Ключ конфига отсутствует: {e}")
            raise
        except psycopg2.Error as e:
            logger.critical(f"Ошибка PostgreSQL [Code {e.pgcode}]: {e.pgerror}")
            raise RuntimeError("Ошибка операций с БД") from e

    def _connect(self) -> None:
        """Установка соединения с таймаутом"""
        try:
            self.conn = psycopg2.connect(
                dbname=DB_CONFIG['dbname'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                connect_timeout=5
            )
            self.conn.set_session(autocommit=False)
            logger.info("Успешное подключение к %s", DB_CONFIG['dbname'])
        except psycopg2.OperationalError as e:
            logger.error("Таймаут подключения: %s", e)
            raise

    def create_tables(self) -> None:
        """Транзакционное создание таблиц с индексами"""
        tables = {
            'employers': """
                CREATE TABLE IF NOT EXISTS employers (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    url VARCHAR(255) UNIQUE,
                    description TEXT,
                    CONSTRAINT valid_url CHECK (url ~ '^https?://')
                )""",
            'vacancies': """
                CREATE TABLE IF NOT EXISTS vacancies (
                    id INTEGER PRIMARY KEY,
                    employer_id INTEGER REFERENCES employers(id) ON DELETE CASCADE,
                    title VARCHAR(255) NOT NULL,
                    salary_from NUMERIC,
                    salary_to NUMERIC,
                    currency VARCHAR(3),
                    url VARCHAR(255) UNIQUE,
                    requirements TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )"""
        }

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_vacancies_employer ON vacancies(employer_id)",
            "CREATE INDEX IF NOT EXISTS idx_vacancies_salary ON vacancies(salary_from, salary_to)"
        ]

        try:
            with self.conn.cursor() as cur:
                for name, ddl in tables.items():
                    cur.execute(ddl)
                    logger.debug("Создана таблица: %s", name)

                for idx in indexes:
                    cur.execute(idx)

                self.conn.commit()
                logger.info("Схема БД успешно инициализирована")
        except psycopg2.DatabaseError as e:
            self.conn.rollback()
            logger.exception("Ошибка при создании схемы")
            raise

    def check_connection(self) -> bool:
        """Проверка работоспособности соединения"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone()[0] == 1
        except psycopg2.InterfaceError:
            return False


if __name__ == "__main__":
    try:
        with DBCreator() as db:
            db.create_tables()
            if db.check_connection():
                print("Успех! Проверьте лог для деталей.")
    except Exception as e:
        logger.exception("Критическая ошибка в основном потоке")