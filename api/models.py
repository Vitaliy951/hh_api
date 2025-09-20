from typing import Dict, Any, Optional
from dataclasses import dataclass
from utils.logger import logger

@dataclass
class Employer:
    """Класс для представления работодателя"""
    id: str
    name: str
    url: str
    open_vacancies: int

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> Optional['Employer']:
        """Создание объекта из JSON-данных API"""
        try:
            return cls(
                id=str(data['id']),
                name=data['name'],
                url=data['alternate_url'],
                open_vacancies=data['open_vacancies']
            )
        except KeyError as e:
            logger.error(f"Отсутствует ключ в данных работодателя: {e}")
            return None

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь для вставки в БД"""
        return {
            'id': self.id,
            'name': self.name,
            'url': self.url,
            'open_vacancies': self.open_vacancies
        }

@dataclass
class Vacancy:
    """Класс для представления вакансии"""
    id: str
    title: str
    employer_id: str
    salary_from: Optional[int]
    salary_to: Optional[int]
    currency: Optional[str]
    url: str

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> Optional['Vacancy']:
        """Создание объекта из JSON-данных API"""
        try:
            salary = data.get('salary')
            return cls(
                id=str(data['id']),
                title=data['name'],
                employer_id=str(data['employer']['id']),
                salary_from=salary.get('from') if salary else None,
                salary_to=salary.get('to') if salary else None,
                currency=salary.get('currency') if salary else None,
                url=data['alternate_url']
            )
        except KeyError as e:
            logger.error(f"Отсутствует ключ в данных вакансии: {e}")
            return None

    def get_avg_salary(self) -> Optional[float]:
        """Рассчет средней зарплаты"""
        if self.salary_from and self.salary_to:
            return (self.salary_from + self.salary_to) / 2
        if self.salary_from:
            return self.salary_from
        if self.salary_to:
            return self.salary_to
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Преобразование в словарь для вставки в БД"""
        return {
            'employer_id': self.employer_id,
            'title': self.title,
            'salary_from': self.salary_from,
            'salary_to': self.salary_to,
            'currency': self.currency,
            'url': self.url
        }