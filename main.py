from api.hh_api import HeadHunterAPI
from database.db_creator import DBCreator
from utils.db_manager import DBManager
from utils.logger import logger


def main():
    logger.info("Starting application")

    # Инициализация БД
    try:
        db_creator = DBCreator()
        db_creator.create_tables()
    except Exception as e:
        logger.critical(f"DB initialization failed: {str(e)}")
        return

    # Получение данных
    hh_api = HeadHunterAPI()
    companies = ['Яндекс', 'Сбербанк']

    logger.info(f"Fetching data for companies: {', '.join(companies)}")
    employers = hh_api.get_employers(companies)

    # Менеджер БД
    db_manager = DBManager()
    stats = db_manager.get_companies_stats()
    logger.info(f"Database stats: {stats}")


if __name__ == "__main__":
    main()
