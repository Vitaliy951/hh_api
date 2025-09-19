"""
Модуль вспомогательных функций для работы с API hh.ru и PostgreSQL
"""
import logging
from typing import Callable, Optional, Dict, List, Tuple, Any


# ==================== Обработка данных ====================
def process_salary(salary_data: Optional[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """Извлекает и форматирует данные о зарплате из ответа API"""
    if not salary_data:
        return (None, None, None)

    return (
        salary_data.get('from'),
        salary_data.get('to'),
        salary_data.get('currency', 'RUR').upper()
    )


def validate_employer_ids(ids: List[str]) -> bool:
    """Проверяет корректность списка ID работодателей"""
    return all(
        isinstance(emp_id, str) and emp_id.isdigit() and len(emp_id) >= 4
        for emp_id in ids
    )


# ==================== Форматирование ====================
def format_vacancy(vacancy: Dict[str, Any]) -> str:
    """Генерирует читаемое представление вакансии"""
    salary_from = vacancy.get('salary_from')
    salary_to = vacancy.get('salary_to')
    currency = vacancy.get('currency', 'RUR')

    salary_info = []
    if salary_from: salary_info.append(f"от {salary_from}")
    if salary_to: salary_info.append(f"до {salary_to}")
    salary_str = " ".join(salary_info) + f" {currency}" if salary_info else "не указана"

    return (
        f"Вакансия: {vacancy['name']}\n"
        f"Компания: {vacancy['employer_name']}\n"
        f"Зарплата: {salary_str}\n"
        f"Ссылка: {vacancy['url']}\n"
    )


# ==================== Логирование ====================
def setup_logger(name: str = 'hh_parser') -> logging.Logger:
    """Настраивает систему логирования"""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler('hh_parser.log', encoding='utf-8')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    return logger


# ==================== Обработка ошибок ====================
def handle_errors(func: Callable) -> Callable:
    """Декоратор для перехвата и логирования исключений"""

    def wrapper(*args, **kwargs):
        logger = setup_logger()
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"Ошибка в {func.__name__}: {str(e)}",
                exc_info=True
            )
            return None

    return wrapper


# ==================== Утилиты БД ====================
def dict_to_sql_params(data: Dict, exclude: List[str] = None) -> Tuple[List[str], List]:
    """Подготавливает параметры для SQL-запроса"""
    exclude = exclude or []
    columns = [k for k in data.keys() if k not in exclude]
    values = [data[col] for col in columns]
    return columns, values


def create_placeholders(num: int) -> str:
    """Генерирует строку с плейсхолдерами для SQL-запроса"""
    return ', '.join(['%s'] * num)


# ==================== Конвертация валют ====================
class CurrencyConverter:
    """Класс для конвертации валют (заглушка)"""

    def __init__(self):
        self.rates = {
            'USD': 90.5,
            'EUR': 99.0,
            'KZT': 0.18
        }

    def convert_to_rub(self, amount: float, currency: str) -> float:
        """Конвертирует сумму в рубли"""
        currency = currency.upper()
        if currency == 'RUR':
            return amount
        return amount * self.rates.get(currency, 1.0)