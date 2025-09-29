import requests
from typing import List
from api.models import Employer, Vacancy
from utils.logger import logger

class HeadHunterAPI:
    BASE_URL = 'https://api.hh.ru/'
    HEADERS = {'User-Agent': 'HH-API Client/1.0'}

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _make_request(self, endpoint: str, params: dict) -> dict:
        try:
            response = self.session.get(
                f'{self.BASE_URL}{endpoint}',
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            return {}

    def get_employers(self, company_names: List[str]) -> List[Employer]:
        employers = []
        for name in company_names:
            data = self._make_request('employers', {'text': name})
            if data.get('items'):
                employer_data = data['items'][0]
                employers.append(Employer.from_json(employer_data))
        return employers

    def get_vacancies(self, employer_id: int) -> List[Vacancy]:
        data = self._make_request('vacancies', {'employer_id': employer_id})
        return [Vacancy.from_json(v) for v in data.get('items', [])]
