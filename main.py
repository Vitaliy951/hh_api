from database.db_manager import DBManager
from database.db_creator import DBCreator
from hh_api import HeadHunterAPI
from helpers import DBHelper
from config import config
from utils.logger import logger


def main():

    db_manager = DBManager(config.db_config)
    db_creator = DBCreator()
    db_creator.create_database()
    db_creator.create_tables()

    # Работа с API
    hh_api = HeadHunterAPI()
    employers = hh_api.get_top_employers()


    if employers:
        db_helper = DBHelper(db_manager)
        inserted = db_helper.insert_employers(
            [emp.to_dict() for emp in employers]
        )
        logger.info(f"Успешно добавлено работодателей: {inserted}")
    else:
        logger.warning("Нет данных работодателей для вставки")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        raise
