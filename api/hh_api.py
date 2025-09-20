import requests
from typing import List, Dict
from utils.logger import logger
from .models import Employer, Vacancy


class HeadHunterAPI:
    def __init__(self):
        self.base_url = "https://api.hh.ru"

    def get_top_employers(self, limit: int = 10) -> list[Employer]:
        """Получение топ компаний по количеству вакансий"""
        params = {
            'area': 1,  # Москва
            'industry': 7,  # IT
            'per_page': limit,
            'sort_by': "by_vacancies_open"
        }
        response = self._make_request('employers', params)
        return [Employer.from_json(item) for item in response['items']]

    def get_employer_vacancies(self, employer_id: str) -> List[Vacancy]:
        """Получает вакансии для конкретного работодателя"""
        try:
            response = requests.get(
                f"{self.base_url}/vacancies",
                params={'employer_id': employer_id, 'per_page': 100}
            )
            response.raise_for_status()
            return [Vacancy(item) for item in response.json()['items']]

        except requests.RequestException as e:
            logger.error(f"Ошибка запроса вакансий: {e}")
            return []