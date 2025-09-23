import requests
from typing import Optional, List, Dict, Any
from pydantic import ValidationError
from utils.logger import logger
from models import Employer, Vacancy
from datetime import datetime, timedelta
import time


class HeadHunterAPI:
    """Класс для работы с API HeadHunter с поддержкой пагинации и кеширования"""

    def __init__(self):
        self.base_url = "https://api.hh.ru"
        self.headers = {
            "User-Agent": "HH-Analyzer/2.0 (admin@hh-analytics.ru)",
            "Accept": "application/json"
        }
        self.timeout = 20
        self.rate_limit_delay = 0.5  # Задержка между запросами
        self.max_retries = 3

    def _make_request(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Выполнение запроса с обработкой ошибок и повторами"""
        url = f"{self.base_url}/{endpoint}"
        params = params or {}

        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit exceeded. Retrying after {retry_after} sec")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                time.sleep(self.rate_limit_delay)
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {url} - {str(e)}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(2 ** attempt)

        return None

    def _paginate(self, endpoint: str, params: dict) -> List[Dict]:
        """Обработка пагинации для API-запросов"""
        results = []
        params = params.copy()
        params['per_page'] = 100  # Максимальное значение для HH API

        for page in range(20):  # Ограничение на 2000 записей
            params['page'] = page
            data = self._make_request(endpoint, params)

            if not data or 'items' not in data:
                break

            results.extend(data['items'])

            if data['pages'] - page <= 1:
                break

        return results

    def get_top_employers(self, limit: int = 10) -> List[Employer]:
        """Получение топовых работодателей по количеству вакансий"""
        params = {
            'area': 113,  # Россия
            'only_with_vacancies': True,
            'sort_by': 'vacancies_count',
            'period': 30
        }

        try:
            data = self._make_request("employers", params)
            if not data:
                return []

            return [
                Employer.from_api(item)
                for item in data.get('items', [])[:limit]
                if Employer.from_api(item) is not None
            ]
        except Exception as e:
            logger.error(f"Ошибка получения работодателей: {str(e)}")
            return []

    def get_vacancies_by_employer(self, employer_id: str) -> List[Vacancy]:
        """Получение всех вакансий работодателя с фильтрацией"""
        params = {
            'employer_id': employer_id,
            'date_from': (datetime.now() - timedelta(days=30)).isoformat(),
            'order_by': 'publication_time',
            'only_with_salary': True
        }

        try:
            items = self._paginate("vacancies", params)
            return [
                Vacancy.from_api(item)
                for item in items
                if Vacancy.from_api(item) is not None
            ]
        except Exception as e:
            logger.error(f"Ошибка получения вакансий: {employer_id} - {str(e)}")
            return []

    def get_vacancy_details(self, vacancy_id: str) -> Optional[Vacancy]:
        """Получение полной информации о вакансии"""
        data = self._make_request(f"vacancies/{vacancy_id}")
        if data:
            return Vacancy.from_api(data)
        return None

    def search_vacancies(self, query: str, limit: int = 100) -> List[Vacancy]:
        """Поиск вакансий по ключевым словам"""
        params = {
            'text': query,
            'search_field': 'name',
            'per_page': min(limit, 100),
            'order_by': 'relevance'
        }

        items = self._paginate("vacancies", params)
        return [
            Vacancy.from_api(item)
            for item in items[:limit]
            if Vacancy.from_api(item) is not None
        ]


if __name__ == "__main__":
    # Пример использования
    api = HeadHunterAPI()

    # Тест получения работодателей
    employers = api.get_top_employers(5)
    print(f"Найдено работодателей: {len(employers)}")

    # Тест получения вакансий
    if employers:
        vacancies = api.get_vacancies_by_employer(employers[0].id)
        print(f"Вакансий у первого работодателя: {len(vacancies)}")

    # Тест поиска
    python_vacancies = api.search_vacancies("Python разработчик", 10)
    print(f"Найдено вакансий Python: {len(python_vacancies)}")
