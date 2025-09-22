import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Dict, Any
import json
from datetime import time


class JSONFormatter(logging.Formatter):
    """Кастомный форматтер для структурированных логов"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
            "logger": record.name
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logger(
        name: str = "app",
        log_level: str = "INFO",
        log_file: str = "logs/app.log",
        max_size: int = 10,  # MB
        backup_count: int = 5
) -> logging.Logger:
    """
    Инициализация продвинутого логгера

    Args:
        name: Имя логгера
        log_level: Уровень логирования (DEBUG, INFO и т.д.)
        log_file: Путь к файлу логов
        max_size: Максимальный размер файла (МБ)
        backup_count: Количество бэкап-файлов
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Уже настроен

    # Создаем директорию для логов
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Уровень логирования
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    # Форматтеры
    console_formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(module)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Обработчики
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(lambda record: record.levelno <= logging.INFO)

    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setFormatter(console_formatter)
    error_handler.setLevel(logging.WARNING)

    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_size * 1024 * 1024,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setFormatter(JSONFormatter())

    # Добавляем обработчики
    logger.addHandler(console_handler)
    logger.addHandler(error_handler)
    logger.addHandler(file_handler)

    # Перехват необработанных исключений
    def handle_exception(exc_type, exc_value, exc_traceback):
        logger.error(
            "Uncaught exception",
            exc_info=(exc_type, exc_value, exc_traceback)
        )

    sys.excepthook = handle_exception

    return logger


# Инициализация логгера с настройками по умолчанию
logger = setup_logger()