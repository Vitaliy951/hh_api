import psycopg2
import os
from dotenv import load_dotenv
from typing import NoReturn
from utils.logger import logger

load_dotenv()


class DBCreator:
    def __init__(self) -> None:
        self.conn_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
        self._validate_credentials()

    def _validate_credentials(self) -> None:
        """Проверка наличия всех необходимых параметров подключения"""
        required = ['dbname', 'user', 'password', 'host', 'port']
        if not all(self.conn_params.get(key) for key in required):
            missing = [key for key in required if not self.conn_params.get(key)]
            raise ValueError(f"Отсутствуют параметры подключения: {', '.join(missing)}")

    def create_database(self) -> None:
        """Создание базы данных если не существует"""
        try:
            conn = psycopg2.connect(
                dbname='postgres',
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                host=self.conn_params['host'],
                connect_timeout=5
            )
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM pg_database 
                    WHERE datname = %s
                """, (self.conn_params['dbname'],))

                if not cursor.fetchone():
                    cursor.execute(f"""
                        CREATE DATABASE {self.conn_params['dbname']}
                        ENCODING 'UTF8'
                        LC_COLLATE 'ru_RU.UTF-8'
                        LC_CTYPE 'ru_RU.UTF-8'
                    """)
                    logger.info(f"Создана БД: {self.conn_params['dbname']}")
                else:
                    logger.info(f"БД {self.conn_params['dbname']} уже существует")
        except psycopg2.OperationalError as e:
            logger.error(f"Ошибка подключения: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    def create_tables(self) -> None:
        """Создание структуры таблиц с индексами и ограничениями"""
        try:
            with psycopg2.connect(**self.conn_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS employers (
                            employer_id VARCHAR(20) PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            url VARCHAR(200),
                            open_vacancies INT,
                            CONSTRAINT valid_url CHECK (url LIKE 'http%')
                        );

                        CREATE INDEX idx_employer_name ON employers(name);
                    ''')

                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS vacancies (
                            vacancy_id VARCHAR(20) PRIMARY KEY,
                            employer_id VARCHAR(20) REFERENCES employers(employer_id)
                                ON DELETE CASCADE,
                            title VARCHAR(200) NOT NULL,
                            salary_from INT,
                            salary_to INT,
                            currency VARCHAR(3),
                            url VARCHAR(200) NOT NULL,
                            CONSTRAINT salary_range CHECK (salary_from <= salary_to),
                            CONSTRAINT valid_vacancy_url CHECK (url LIKE 'http%')
                        );

                        CREATE INDEX idx_vacancy_salary ON vacancies (
                            COALESCE(salary_from, salary_to)
                        );
                    ''')

                    logger.info("Структура БД успешно создана")
        except psycopg2.Error as e:
            logger.error(f"Ошибка создания таблиц: {e}")
            raise


if __name__ == "__main__":
    db_creator = DBCreator()
    db_creator.create_database()
    db_creator.create_tables()