import logging
import sys


def setup_logger(name: str = 'hh_parser'):
    """Настройка централизованного логгера"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Консольный вывод
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Файловый вывод
    file_handler = logging.FileHandler('app.log')
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# Инициализация логгера для импорта
logger = setup_logger()