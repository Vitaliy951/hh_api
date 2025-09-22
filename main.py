from database.db_manager import DBManager
from database.db_creator import create_database, create_tables
from api.hh_api import HeadHunterAPI
from utils.helpers import insert_employers, insert_vacancies
from config import config
import time


def main():
    # Инициализация БД
    create_database()
    create_tables()

    # Получение данных
    hh_api = HeadHunterAPI()
    employers = hh_api.get_top_employers()

    # Сохранение данных
    insert_employers(employers)

    vacancies = []
    for employer in employers:
        time.sleep(0.5)  # Задержка для соблюдения лимитов API
        vacancies += hh_api.get_vacancies_by_employer(employer.id)

    insert_vacancies(vacancies)

    # Работа с пользователем
    db = DBManager(config.db_config)
    user_menu(db)


def user_menu(db: DBManager):
    while True:
        print("\n=== Меню анализа вакансий ===")
        print("1. Список компаний и количество вакансий")
        print("2. Показать все вакансии")
        print("3. Средняя зарплата")
        print("4. Вакансии с зарплатой выше средней")
        print("5. Поиск по ключевым словам")
        print("0. Выход")

        choice = input("Выберите действие: ").strip()

        if choice == '1':
            print("\nКомпании и количество вакансий:")
            for company, count in db.get_companies_and_vacancies_count():
                print(f"- {company}: {count} вакансий")

        elif choice == '2':
            print("\nВсе вакансии:")
            for company, title, salary_from, salary_to, url in db.get_all_vacancies():
                salary = format_salary(salary_from, salary_to)
                print(f"{company} | {title} | {salary} | {url}")

        elif choice == '3':
            avg = db.get_avg_salary()
            print(f"\nСредняя зарплата: {avg:.2f} руб.")

        elif choice == '4':
            print("\nВакансии с зарплатой выше средней:")
            for title, salary, url in db.get_vacancies_with_higher_salary():
                print(f"- {title}: {salary} руб. | {url}")

        elif choice == '5':
            keyword = input("Введите ключевое слово: ").strip()
            print(f"\nРезультаты поиска по '{keyword}':")
            for title, url in db.get_vacancies_with_keyword(keyword):
                print(f"- {title} | {url}")

        elif choice == '0':
            print("Выход из программы")
            break

        else:
            print("Неверный ввод, попробуйте снова")


def format_salary(from_: int, to_: int) -> str:
    if from_ and to_:
        return f"{from_} - {to_} руб."
    return f"{from_ or to_ or '???'} руб."


if __name__ == "__main__":
    main()
