import os
from typing import Dict, Any
from dotenv import load_dotenv


def validate_db_config(config: Dict[str, Any]):
    required = {'dbname', 'user', 'password', 'host', 'port'}
    if missing := required - config.keys():
        raise ValueError(f"Missing keys: {missing}")

    if not isinstance(config['port'], int):
        raise TypeError("Port must be integer")


# Вызов проверки
validate_db_config(DB_CONFIG)


load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': 5432
}