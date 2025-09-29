import psycopg2
from config import DB_CONFIG


def create_tables():
    """Создает структуру БД"""
    commands = (
        """
        CREATE TABLE employers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            hh_id INTEGER UNIQUE
        )
        """,
        """
        CREATE TABLE vacancies (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            salary_from INTEGER,
            salary_to INTEGER,
            url VARCHAR(255),
            employer_id INTEGER REFERENCES employers(id) ON DELETE CASCADE
        )
        """
    )

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
            conn.commit()
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")