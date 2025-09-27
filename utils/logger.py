import logging
import sys
import json
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
import traceback


class EnhancedJSONFormatter(logging.Formatter):
    """Расширенный JSON-форматтер с дополнительными метаданными"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process": record.process,
            "thread": record.thread,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "stack": traceback.format_tb(record.exc_info[2])
            }

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class ColorConsoleFormatter(logging.Formatter):
    """Цветное форматирование для консоли"""

    COLORS = {
        'DEBUG': '\033[94m',  # Blue
        'INFO': '\033[92m',  # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',  # Red
        'CRITICAL': '\033[95m'  # Purple
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, '')
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


def setup_logger(
        name: str = "app",
        log_level: str = "INFO",
        log_dir: str = "logs",
        max_file_size: int = 10,  # MB
        backup_days: int = 7,
        enable_console: bool = True
) -> logging.Logger:
    """
    Усовершенствованная инициализация логгера

    Args:
        name: Имя логгера
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Директория для хранения логов
        max_file_size: Максимальный размер файла (МБ)
        backup_days: Хранение логов за N дней
        enable_console: Включить вывод в консоль
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Уже настроен

    # Валидация уровня логирования
    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logger.setLevel(level)

    # Создание директории для логов
    log_path = Path(log_dir)
    try:
        log_path.mkdir(parents=True, exist_ok=True)
        if not os.access(log_path, os.W_OK):
            raise PermissionError(f"No write permissions for: {log_path}")
    except Exception as e:
        sys.stderr.write(f"Logger initialization error: {str(e)}\n")
        raise

    # Форматтеры
    json_formatter = EnhancedJSONFormatter()
    console_formatter = ColorConsoleFormatter(
        "[%(asctime)s] %(levelname)-8s %(module)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Обработчики
    handlers = []

    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.addFilter(lambda r: r.levelno <= logging.INFO)

        error_handler = logging.StreamHandler(sys.stderr)
        error_handler.setFormatter(console_formatter)
        error_handler.setLevel(logging.WARNING)

        handlers.extend([console_handler, error_handler])

    # Ротируемый файловый обработчик
    file_handler = TimedRotatingFileHandler(
        filename=log_path / "app.log",
        when="midnight",
        interval=1,
        backupCount=backup_days,
        encoding="utf-8"
    )
    file_handler.setFormatter(json_formatter)
    file_handler.addFilter(lambda r: r.levelno >= logging.INFO)
    handlers.append(file_handler)

    # Добавление обработчиков
    for handler in handlers:
        logger.addHandler(handler)

    # Перехват необработанных исключений
    def exception_handler(exc_type, exc_value, exc_traceback):
        logger.critical(
            "Unhandled exception",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = exception_handler

    return logger


# Инициализация логгера по умолчанию
logger = setup_logger()
