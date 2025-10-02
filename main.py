import sys
from database.db_manager import DBManager
from hh_api import load_data_to_db
from utils.logger import logger

def user_interface():
    """Интерфейс взаимодействия с пользователем"""
    with DBManager() as db:
        while True:
            print("\n1. ТОП-10 компаний по вакансиям")
            print("2. Все вакансии")
            print("3. Средняя зарплата")
            print("4. Вакансии с высокой ЗП")
            print("5. Поиск вакансий")
            print("0. Выход")

            choice = input("Выберите действие: ")

            if choice == "1":
                print("\nКомпания | Открытых вакансий")
                for company, count in db.get_companies_and_vacancies_count():
                    print(f"{company}: {count}")

            elif choice == "2":
                print("\nВсе вакансии:")
                for vac in db.get_all_vacancies():
                    print(vac)

            elif choice == "3":
                avg = db.get_avg_salary()
                print(f"\nСредняя зарплата: {avg} RUB")

            elif choice == "4":
                print("\nВакансии с зарплатой выше средней:")
                for vac in db.get_vacancies_with_higher_salary():
                    print(f"{vac[0]} | {vac[1]}-{vac[2]} RUB | {vac[3]}")

            elif choice == "5":
                keyword = input("Введите ключевое слово: ")
                results = db.get_vacancies_with_keyword(keyword)
                print(f"\nНайдено {len(results)} вакансий:")
                for vac in results:
                    print(f"{vac[0]} | {vac[1]}-{vac[2]} RUB | {vac[3]}")

            elif choice == "0":
                break
            else:
                print("Некорректный ввод!")

if __name__ == "__main__":
    try:
        force_flag = '--force' in sys.argv
        load_data_to_db(force_flag)
        user_interface()
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}")
        raise