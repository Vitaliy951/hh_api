import requests
from typing import Dict, List
import time


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
        """Улучшенная версия с повторами и таймаутами"""
        params = {'employer_id': employer_id, 'per_page': 100}
        for _ in range(3):
            try:
                response = requests.get(
                    f"{self.BASE_URL}vacancies",
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                return response.json().get('items', [])
            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка запроса: {str(e)}")
                time.sleep(2)
        return []