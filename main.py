from database.db_manager import DBManager
from api.hh_api import HeadHunterAPI
from database.models import Employer, Vacancy
import os


def main():
    # Инициализация БД
    db = DBManager(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST')
    )

    # Получение данных
    api = HeadHunterAPI(employer_ids=['15478', '2180', ...])  # 10 компаний
    employers = api.get_employers()
    vacancies = []
    for emp in employers:
        vacancies.extend(api.get_vacancies(emp['id']))

    # Заполнение БД
    with db._get_cursor() as cur:
        # Вставка работодателей
        for emp in employers:
            cur.execute(
                "INSERT INTO employers VALUES (%s, %s, %s, %s)",
                (emp['id'], emp['name'], emp['url'], emp['open_vacancies'])
            )

        # Вставка вакансий
        for vac in vacancies:
            salary = vac.get('salary')
            cur.execute(
                """INSERT INTO vacancies 
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    vac['id'],
                    vac['name'],
                    vac['employer']['id'],
                    salary['from'] if salary else None,
                    salary['to'] if salary else None,
                    salary['currency'] if salary else None,
                    vac['alternate_url']
                )
            )

    # Пример использования
    print("Компании и количество вакансий:")
    for company, count in db.get_companies_and_vacancies_count():
        print(f"{company}: {count} вакансий")


if __name__ == "__main__":
    main()
