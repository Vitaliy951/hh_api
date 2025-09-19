# Проект: Анализ вакансий с hh.ru и управление данными в PostgreSQL

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-brightgreen)](https://postgresql.org)

## 📖 Оглавление
1. [Тематика проекта](#-тематика-проекта)
2. [Особенности](#-особенности)
3. [Установка](#-установка)
4. [Структура проекта](#-структура-проекта)
5. [Примеры использования](#-примеры-использования)
6. [Типизация и документация](#-типизация-и-документация)
7. [SQL-запросы](#-sql-запросы)
8. [Лицензия](#-лицензия)

## 🎯 Тематика проекта
Проект предназначен для:
- Сбора данных о компаниях и вакансиях через публичное API hh.ru
- Автоматизации работы с реляционной базой данных PostgreSQL
- Анализа рыночных предложений по вакансиям
- Построения статистики по зарплатам

## ✨ Особенности
```text
✅ Парсинг данных с hh.ru через REST API
✅ Автоматическое создание структуры БД
✅ Расширенная фильтрация вакансий
✅ Расчет средней зарплаты по выборке
✅ Поиск по ключевым словам в названиях
```

## ⚙️ Установка
1. Клонировать репозиторий:
```bash
git clone https://github.com/yourusername/hh-vacancies-analysis.git
```
2. Установить зависимости:
```bash
pip install -r requirements.txt
```
3. Настроить базу данных:
```bash
createdb hh_vacancies
export DB_PASSWORD=yourpassword && export DB_USER=postgres
```

## 📁 Структура проекта
```tree
hh-api/
├── api/
│   ├── hh_api.py        # Модуль работы с API HeadHunter
│   └── config.py        # Конфигурационные параметры
├── database/
│   ├── db_manager.py    # Класс для управления БД
│   └── models.py        # Модели данных Pydantic
├── utils/
│   └── validators.py    # Валидация входных данных
├── main.py              # Точка входа
├── .env.example         # Шаблон конфигурации
└── queries.sql          # Примеры SQL-запросов
```

## 🖥 Примеры использования
### Запуск основного скрипта
```python
python main.py --employers 15478,2180,78638 --depth 3
```

### Пример вывода
```text
Топ-5 компаний по количеству вакансий:
1. Яндекс: 127 вакансий
2. Сбер: 89 вакансий
3. Tinkoff: 75 вакансий

Средняя зарплата: 156 ₽
Вакансии с зарплатой выше средней: 23 позиции
```

## 📝 Типизация и документация
### Аннотации типов
```python
def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, int]]:
    """Возвращает вакансии, содержащие ключевое слово"""
```

### Документация классов
```python
class DBManager:
    """Основной класс для управления базой данных
    
    Attributes:
        conn_params (dict): Параметры подключения к БД
        cursor (psycopg2.extensions.cursor): Курсор для выполнения запросов
    """
```

## 🗃 SQL-запросы
### Создание таблиц
```sql
CREATE TABLE employers (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL CHECK (name <> ''),
    url VARCHAR(200) CHECK (url LIKE 'https://%')
);
```

### Пример сложного запроса
```sql
SELECT 
    e.name AS company,
    COUNT(v.id) AS vacancies,
    AVG((v.salary_from + v.salary_to)/2)::INTEGER AS avg_salary
FROM employers e
LEFT JOIN vacancies v ON e.id = v.employer_id
GROUP BY e.id
ORDER BY avg_salary DESC NULLS LAST;
```

## 📜 Лицензия
Проект распространяется под лицензией [MIT](LICENSE). При использовании данных с hh.ru соблюдайте [правила API](https://dev.hh.ru/terms).

[⬆️ К оглавлению](#-оглавление)
