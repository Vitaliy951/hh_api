import os
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()  # Переносим загрузку .env в начало


def validate_db_config(config: Dict[str, Any]):
    required = {'dbname', 'user', 'password', 'host', 'port'}
    if missing := required - config.keys():
        raise ValueError(f"Отсутствуют ключи: {missing}")

    if not isinstance(config['port'], int):
        raise TypeError("Порт должен быть целым числом")


DB_CONFIG = {
    'dbname': os.getenv("DB_NAME", "postgres"),  # Значение по умолчанию
    'user': os.getenv("DB_USER", "postgres"),
    'password': os.getenv("DB_PASSWORD", ""),
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", "5432"))  # Конвертация в integer
}

validate_db_config(DB_CONFIG)  # Вызываем после создания конфига
