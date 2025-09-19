import pytest
from database.db_manager import DBManager
from config import load_config

@pytest.fixture
def db():
    config = load_config()
    return DBManager(
        dbname=config['DB_NAME'],
        user=config['DB_USER'],
        password=config['DB_PASSWORD'],
        host=config['DB_HOST']
    )

def test_companies_count(db):
    result = db.get_companies_and_vacancies_count()
    assert len(result) > 0
    for company, count in result:
        assert isinstance(company, str)
        assert isinstance(count, int)