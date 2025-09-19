import requests
from typing import Dict, List


class HeadHunterAPI:
    """Класс для взаимодействия с API HeadHunter"""

    BASE_URL = "https://api.hh.ru/"

    def __init__(self, employer_ids: List[str]):
        self.employer_ids = employer_ids

    def get_employers(self) -> List[Dict]:
        """Получение данных о компаниях"""
        employers = []
        for emp_id in self.employer_ids:
            response = requests.get(f"{self.BASE_URL}employers/{emp_id}")
            if response.status_code == 200:
                data = response.json()
                employers.append({
                    'id': data['id'],
                    'name': data['name'],
                    'url': data['alternate_url'],
                    'open_vacancies': data['open_vacancies']
                })
        return employers

    def get_vacancies(self, employer_id: str) -> List[Dict]:
        """Получение вакансий компании"""
        params = {'employer_id': employer_id, 'per_page': 100}
        response = requests.get(f"{self.BASE_URL}vacancies", params=params)
        return response.json().get('items', []) if response.ok else []
