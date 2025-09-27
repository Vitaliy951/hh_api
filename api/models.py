from datetime import datetime
from pydantic import BaseModel, Field, validator, HttpUrl, ValidationError
from typing import Optional, Dict, Any
from utils.logger import logger
import re


class Employer(BaseModel):
    """Модель работодателя с валидацией данных"""
    id: str = Field(..., min_length=3, max_length=20, regex=r'^\d+$')
    name: str = Field(..., min_length=2, max_length=100)
    url: Optional[HttpUrl] = None
    open_vacancies: Optional[int] = Field(None, ge=0)
    description: Optional[str] = Field(None, max_length=2000)
    trusted: Optional[bool] = False

    @validator('name')
    def validate_name(cls, value):
        if re.search(r'[!@#$%^&*()+=}{\[\]|\\:;"\'<>?/~`]', value):
            raise ValueError('Некорректные символы в названии компании')
        return value.strip()

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> Optional['Employer']:
        """Создание объекта из API-ответа"""
        try:
            return cls(
                id=str(data['id']),
                name=data['name'],
                url=data.get('alternate_url'),
                open_vacancies=data.get('open_vacancies'),
                description=data.get('description'),
                trusted=data.get('trusted', False)
            )
        except (KeyError, TypeError) as e:
            logger.error(f"Ошибка парсинга работодателя: {str(e)} | Данные: {data}")
            return None


class Vacancy(BaseModel):
    """Модель вакансии с расширенной валидацией"""
    id: str = Field(..., min_length=5, max_length=20, regex=r'^\d+$')
    title: str = Field(..., min_length=5, max_length=200)
    salary_from: Optional[int] = Field(None, ge=0)
    salary_to: Optional[int] = Field(None, ge=0)
    employer: Employer
    url: HttpUrl
    currency: Optional[str] = Field(None, regex=r'^[A-Z]{3}$')
    experience: Optional[str] = Field(None, max_length=50)
    schedule: Optional[str] = Field(None, max_length=50)
    published_at: Optional[datetime] = None
    skills: Optional[str] = None

    @validator('salary_to')
    def validate_salary_range(cls, v, values):
        if 'salary_from' in values and v and values['salary_from']:
            if v < values['salary_from']:
                raise ValueError('Верхняя граница зарплаты меньше нижней')
        return v

    @validator('title')
    def validate_title(cls, value):
        value = value.strip()
        if len(value) < 5:
            raise ValueError('Слишком короткое название вакансии')
        return value

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> Optional['Vacancy']:
        """Создание объекта из API-ответа HeadHunter"""
        try:
            salary = data.get('salary') or {}
            employer_data = data.get('employer') or {}

            return cls(
                id=str(data['id']),
                title=data['name'],
                salary_from=salary.get('from'),
                salary_to=salary.get('to'),
                employer=Employer.from_api(employer_data),
                url=data['alternate_url'],
                currency=salary.get('currency'),
                experience=data.get('experience', {}).get('name'),
                schedule=data.get('schedule', {}).get('name'),
                published_at=datetime.fromisoformat(data['published_at']) if data.get('published_at') else None,
                skills='; '.join([s['name'] for s in data.get('key_skills', [])])
            )
        except (KeyError, ValueError, ValidationError) as e:
            logger.error(
                f"Ошибка парсинга вакансии: {str(e)} | "
                f"ID: {data.get('id')} | "
                f"Данные: {data}"
            )
            return None

    def normalized_salary(self) -> Optional[float]:
        """Рассчет нормализованной зарплаты"""
        if self.salary_from and self.salary_to:
            return (self.salary_from + self.salary_to) / 2
        if self.salary_from:
            return self.salary_from * 1.2
        if self.salary_to:
            return self.salary_to * 0.8
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь для БД"""
        return {
            'vacancy_id': self.id,
            'title': self.title,
            'salary_from': self.salary_from,
            'salary_to': self.salary_to,
            'employer_id': self.employer.id,
            'url': str(self.url),
            'currency': self.currency,
            'experience': self.experience,
            'schedule': self.schedule,
            'published_at': self.published_at,
            'skills': self.skills
        }


if __name__ == "__main__":
    # Пример использования
    test_data = {
        'id': '12345',
        'name': 'Python Developer (Django)',
        'salary': {
            'from': 150000,
            'to': 220000,
            'currency': 'RUR'
        },
        'employer': {
            'id': '54321',
            'name': 'TechCorp',
            'alternate_url': 'https://hh.ru/employer/54321',
            'open_vacancies': 5
        },
        'alternate_url': 'https://hh.ru/vacancy/12345',
        'experience': {'name': 'От 1 года'},
        'schedule': {'name': 'Удаленная работа'},
        'published_at': '2025-09-23T10:00:00+0300',
        'key_skills': [{'name': 'Python'}, {'name': 'Django'}]
    }

    try:
        vacancy = Vacancy.from_api(test_data)
        if vacancy:
            print(f"Успешно создана вакансия: {vacancy.title}")
            print(f"Работодатель: {vacancy.employer.name}")
            print(f"Нормализованная зарплата: {vacancy.normalized_salary():.0f} {vacancy.currency}")
    except Exception as e:
        logger.error(f"Тестовый пример завершился ошибкой: {str(e)}")
