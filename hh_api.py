import requests
import argparse
from typing import List, Dict
from config import DB_CONFIG
from database.db_creator import DBCreator
from database.db_manager import DBManager
from utils.logger import logger

EMPLOYER_IDS = ['15478', '1740', '3529', '78638', '87021', '2180', '3776', '39305', '64174', '1122462']


def get_employer_data(employer_id: str) -> Dict:
    url = f'https://api.hh.ru/employers/{employer_id}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка получения данных работодателя {employer_id}: {str(e)}")
        return None


def get_vacancies(employer_id: str) -> List[Dict]:
    try:
        response = requests.get(
            f'https://api.hh.ru/vacancies?employer_id={employer_id}&per_page=100',
            timeout=15
        )
        response.raise_for_status()
        return response.json().get('items', [])
    except Exception as e:
        logger.error(f"Ошибка получения вакансий для {employer_id}: {str(e)}")
        return []


def save_to_database(employers: List[Dict], vacancies: List[Dict]):
    with DBManager() as db:
        try:
            for employer in employers:
                db.insert_employer(
                    hh_id=employer['id'],
                    name=employer['name'],
                    description=employer.get('description'),
                    area=employer['area']['name'] if employer.get('area') else None,
                    open_vacancies=employer.get('open_vacancies')
                )

                employer_vacancies = [
                    v for v in vacancies
                    if v['employer']['id'] == employer['id']
                ]

                for vacancy in employer_vacancies:
                    salary = vacancy.get('salary', {})
                    db.insert_vacancy(
                        hh_id=vacancy['id'],
                        title=vacancy['name'],
                        salary_from=salary.get('from'),
                        salary_to=salary.get('to'),
                        currency=salary.get('currency'),
                        experience=vacancy['experience']['name'] if vacancy.get('experience') else None,
                        schedule=vacancy['schedule']['name'] if vacancy.get('schedule') else None,
                        employer_hh_id=employer['id'],
                        url=vacancy['alternate_url']
                    )
            logger.info(f"Успешно сохранено: {len(employers)} компаний и {len(vacancies)} вакансий")
        except Exception as e:
            logger.critical(f"Ошибка сохранения данных: {str(e)}")
            raise


def load_data_to_db(force: bool = False):
    try:
        # Инициализация БД
        with DBCreator() as db_creator:
            db_creator.create_tables()

        with DBManager() as db:
            if not force and db.has_data():
                print("Данные уже загружены. Используйте --force для перезаписи.")
                return

            if force:
                db.clear_tables()

            employers = []
            vacancies = []
            for eid in EMPLOYER_IDS:
                if emp := get_employer_data(eid):
                    employers.append(emp)
                    vacancies.extend(get_vacancies(eid))

            save_to_database(employers, vacancies)
    except Exception as e:
        logger.error(f"Критическая ошибка при загрузке данных: {str(e)}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='Принудительная перезагрузка данных')
    args = parser.parse_args()
    load_data_to_db(args.force)