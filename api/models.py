class Employer:
    def __init__(self, id: int, name: str, url: str):
        self.id = id
        self.name = name
        self.url = url

    @classmethod
    def from_json(cls, data: dict):
        return cls(
            id=data['id'],
            name=data['name'],
            url=data['alternate_url']
        )

class Vacancy:
    def __init__(self, title: str, salary: int, url: str, employer_id: int):
        self.title = title
        self.salary = salary or 0
        self.url = url
        self.employer_id = employer_id

    @classmethod
    def from_json(cls, data: dict):
        return cls(
            title=data['name'],
            salary=data.get('salary', {}).get('from'),
            url=data['alternate_url'],
            employer_id=data['employer']['id']
        )