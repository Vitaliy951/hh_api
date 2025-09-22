from dataclasses import dataclass
from typing import Optional
from utils.logger import logger

@dataclass
class Vacancy:
    id: str
    title: str
    salary_from: Optional[int]
    salary_to: Optional[int]
    employer_id: str
    url: str
    currency: Optional[str] = None

    @classmethod
    def from_json(cls, data: dict) -> Optional['Vacancy']:
        try:
            salary = data.get('salary', {})
            return cls(
                id=str(data['id']),
                title=data['name'],
                salary_from=salary.get('from'),
                salary_to=salary.get('to'),
                employer_id=str(data['employer']['id']),
                url=data['alternate_url'],
                currency=salary.get('currency')
            )
        except KeyError as e:
            logger.error(f"Vacancy parse error: Missing key {e}")
            return None
