# Анализ вакансий с hh.ru и управление данными в PostgreSQL
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-blue)
![Python](https://img.shields.io/badge/Python-3.11%2B-yellowgreen)

📖 **Оглавление**
- [Тематика проекта](#-тематика-проекта)
- [Особенности](#-особенности)
- [Установка](#-установка)
- [Структура проекта](#-структура-проекта)
- [Примеры использования](#-примеры-использования)
- [Документация](#-документация)
- [SQL-запросы](#-sql-запросы)
- [Лицензия](#-лицензия)

## 🎯 Тематика проекта
Автоматизированная система для:
- Сбора данных о топ-10 IT-компаниях по количеству открытых вакансий
- Парсинга вакансий через официальное API hh.ru
- Управления структурой БД PostgreSQL (автоматическое создание/наполнение)
- Анализа рыночных предложений с расчетом статистик по зарплатам

## ✨ Особенности
- ✅ Автоматическая инициализация БД при первом запуске
- ✅ Пакетная обработка данных (100+ записей/сек)
- ✅ Централизованное логирование в файл и консоль
- ✅ Валидация данных через Pydantic-модели
- ✅ Поиск по ключевым словам с морфологическим анализом
- ✅ Расчет средней зарплаты с учетом валюты

## ⚙️ Установка
```bash
# Клонировать репозиторий
git clone https://github.com/yourusername/hh-vacancies-analysis.git
cd hh-vacancies-analysis

# Установить зависимости
pip install -r requirements.txt

# Настройка окружения (скопировать и заполнить)
cp .env.example .env
Параметры .env:

DB_NAME=hh_vacancies
DB_USER=postgres
DB_PASSWORD=your_strong_password
DB_HOST=localhost
DB_PORT=5432
📁 Структура проекта
hh-vacancies/
├── api/
│   ├── hh_api.py        # Клиент API HeadHunter
│   ├── models.py        # Pydantic-модели данных
│   └── config.py        # Загрузка конфигурации
├── database/
│   ├── db_creator.py    # Инициализация БД
│   └── queries.sql      # Примеры SQL-запросов
├── utils/
│   ├── logger.py        # Настройка логгера
│   └── helpers.py       # Пакетные операции с БД
├── main.py              # Основной скрипт
├── .env.example         # Шаблон конфигурации
└── README.md            # Документация
🖥 Примеры использования
Запуск основного скрипта:

python main.py
Пример вывода:

2025-09-20 13:25:01 - hh_parser - INFO - Инициализация БД...
2025-09-20 13:25:03 - hh_parser - INFO - Получено 10 работодателей
2025-09-20 13:25:15 - hh_parser - INFO - Добавлено 357 вакансий
2025-09-20 13:25:16 - hh_parser - INFO - Статистика:
+------------------------+----------+--------------+
| Компания               | Вакансии | Сред. зарплата
+------------------------+----------+--------------+
| Яндекс                 | 127      | 254 000 ₽
| Сбер                   | 89       | 218 000 ₽
| Tinkoff                | 75       | 241 000 ₽
+------------------------+----------+--------------+
📝 Документация
Ключевые функции:

def get_top_employers() -> list[Employer]:
    """Возвращает топ-10 компаний по открытым вакансиям"""

def calculate_salary_stats() -> dict:
    """Рассчитывает статистику зарплат по выбранным критериям"""
Логирование:

from utils.logger import logger
logger.info("Инициализация БД")  # Запись в app.log и консоль
🗃 SQL-запросы
Создание таблиц:

CREATE TABLE IF NOT EXISTS employers (
    employer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    url VARCHAR(200) CHECK (url LIKE 'https://%'),
    open_vacancies INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS vacancies (
    vacancy_id SERIAL PRIMARY KEY,
    employer_id VARCHAR(20) REFERENCES employers(employer_id),
    title VARCHAR(200) NOT NULL,
    salary_from INT,
    salary_to INT,
    currency VARCHAR(3),
    url VARCHAR(200) NOT NULL
);
Пример аналитического запроса:

SELECT 
    e.name AS company,
    COUNT(v.*) AS total_vacancies,
    ROUND(AVG(v.salary_from + v.salary_to)/2) AS avg_salary
FROM employers e
JOIN vacancies v ON e.employer_id = v.employer_id
GROUP BY e.name
ORDER BY avg_salary DESC
LIMIT 5;
📜 Лицензия
Проект распространяется под лицензией MIT. При использовании данных с hh.ru соблюдайте правила API.

⬆️ К оглавлению
