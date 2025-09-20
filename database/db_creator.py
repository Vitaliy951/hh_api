import psycopg2
import os
from dotenv import load_dotenv
from utils.logger import logger

load_dotenv()


class DBCreator:
    def __init__(self):
        self.conn_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }

    def create_database(self):
        """Создание базы данных если не существует"""
        try:
            conn = psycopg2.connect(
                dbname='postgres',
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                host=self.conn_params['host']
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{self.conn_params['dbname']}'")
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(f"CREATE DATABASE {self.conn_params['dbname']}")
                logger.info(f"База данных {self.conn_params['dbname']} создана")
            else:
                logger.info(f"База данных {self.conn_params['dbname']} уже существует")

            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Ошибка при создании БД: {e}")
            raise

    def create_tables(self):
        """Создание таблиц в базе данных"""
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS employers (
                            employer_id VARCHAR(20) PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            url VARCHAR(200),
                            open_vacancies INT
                        );
                    ''')
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS vacancies (
                            vacancy_id VARCHAR(20) PRIMARY KEY,
                            employer_id VARCHAR(20) REFERENCES employers(employer_id),
                            title VARCHAR(200) NOT NULL,
                            salary_from INT,
                            salary_to INT,
                            currency VARCHAR(3),
                            url VARCHAR(200) NOT NULL
                        );
                    ''')
                    logger.info("Таблицы employers и vacancies созданы")
                except Exception as e:
                    logger.error(f"Ошибка при создании таблиц: {e}")
                    raise