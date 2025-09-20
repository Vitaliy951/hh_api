import os
from dotenv import load_dotenv
from database.db_creator import DBCreator
from api.hh_api import HeadHunterAPI
from api.models import Employer, Vacancy
from utils.logger import logger
from utils.helpers import batch_insert

load_dotenv()


def main():
    # Инициализация БД
    db_creator = DBCreator()
    db_creator.create_database()
    db_creator.create_tables()

    # Получение топ-10 компаний
    hh_api = HeadHunterAPI()
    top_employers = hh_api.get_top_employers(limit=10)

    # Сохранение работодателей
    employers_data = [e.to_dict() for e in top_employers if e]
    batch_insert('employers', employers_data)

    # Получение и сохранение вакансий
    for employer in top_employers:
        vacancies = hh_api.get_vacancies_by_employer(employer.id)
        vacancies_data = [v.to_dict() for v in vacancies if v]
        batch_insert('vacancies', vacancies_data)

    logger.info("Данные успешно загружены")


if __name__ == "__main__":
    main()