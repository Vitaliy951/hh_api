"""
Модуль конфигурации приложения
"""
import os
from typing import Dict, Any
from dotenv import load_dotenv
import logging

# Инициализация логгера
logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Загрузка и валидация конфигурации"""
    load_dotenv()  # Загружаем переменные из .env

    required_keys = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST']
    config = {key: os.getenv(key) for key in required_keys}

    missing = [k for k, v in config.items() if not v]
    if missing:
        error_msg = f"Отсутствуют переменные окружения: {', '.join(missing)}"
        logger.error(error_msg)
        raise EnvironmentError(error_msg)

    config.update({
        'API_BASE_URL': 'https://api.hh.ru/',
        'REQUEST_TIMEOUT': 10,
        'MAX_RETRIES': 3
    })

    return config


# Пример использования
if __name__ == "__main__":
    try:
        conf = load_config()
        print("Конфигурация успешно загружена:")
        print(f"DB: {conf['DB_USER']}@{conf['DB_HOST']}/{conf['DB_NAME']}")
    except Exception as e:
        print(f"Ошибка конфигурации: {str(e)}")