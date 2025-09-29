import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'dbname': 'hh_vacancies',
    'user': 'ваш_пользователь_СУБД',
    'password': 'пароль',
    'host': 'localhost',  # Попробуйте 127.0.0.1 если не работает
    'port': 5432
}