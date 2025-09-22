import requests
from typing import Optional, List
from utils.logger import logger
from .models import Employer, Vacancy

class HeadHunterAPI:
    def __init__(self):
        self.base_url = "https://api.hh.ru"
        self.headers = {"User-Agent": "HH-Analyzer/1.0"}

    def _make_request(self, endpoint: str, params: dict = None) -> Optional[dict]:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {url} - {str(e)}")
            return None

    def get_top_employers(self, limit: int = 10) -> List[Employer]:
        params = {
            'per_page': limit,
            'sort_by': 'by_vacancies_open',
            'only_with_vacancies': True
        }
        data = self._make_request("employers", params)
        return [Employer.from_json(item) for item in data.get('items', [])] if data else []

    def get_vacancies_by_employer(self, employer_id: str) -> List[Vacancy]:
        params = {'employer_id': employer_id, 'per_page': 100}
        data = self._make_request("vacancies", params)
        return [Vacancy.from_json(item) for item in data.get('items', [])] if data else []
