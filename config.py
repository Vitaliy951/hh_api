import os
from dotenv import load_dotenv
from typing import Dict

def config() -> Dict[str, str]:
    """Загрузка конфигурации БД из переменных окружения"""
    load_dotenv()
    return {
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }