from dataclasses import dataclass

@dataclass
class Employer:
    id: str
    name: str
    url: str
    open_vacancies: int

@dataclass
class Vacancy:
    id: str
    name: str
    employer_id: str
    salary_from: int
    salary_to: int
    currency: str
    url: str
