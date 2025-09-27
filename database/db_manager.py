import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from typing import List, Dict, Optional, Callable, Any, Union
from contextlib import contextmanager
from config import config
from utils.logger import logger


class DBManager:
    """Менеджер для работы с PostgreSQL с поддержкой транзакций и пуллинга"""

    def __init__(self, pool_size: int = 5) -> None:
        self._params = self._load_and_validate_config()
        self.pool_size = pool_size
        self._connection_pool: List[connection] = []

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Загрузка и валидация конфигурации БД"""
        params = config()
        required = {
            'dbname': str,
            'user': str,
            'password': str,
            'host': str,
            'port': int
        }

        for key, type_ in required.items():
            if key not in params:
                raise ValueError(f"Missing required config key: {key}")
            if not isinstance(params[key], type_):
                raise TypeError(f"Invalid type for {key}: expected {type_.__name__}")

        return params

    @contextmanager
    def _get_connection(self) -> connection:
        """Контекстный менеджер для получения соединения из пула"""
        conn = None
        try:
            if self._connection_pool:
                conn = self._connection_pool.pop()
            else:
                conn = psycopg2.connect(
                    **self._params,
                    connect_timeout=5,
                    options=f"-c statement_timeout={30 * 1000}"  # 30 секунд
                )
            yield conn
        except psycopg2.OperationalError as e:
            logger.critical(f"Connection failed: {str(e)}")
            raise
        finally:
            if conn and len(self._connection_pool) < self.pool_size:
                self._connection_pool.append(conn)

    @contextmanager
    def _get_cursor(self) -> cursor:
        """Контекстный менеджер для работы с курсором"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    yield cur
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Transaction rolled back: {str(e)}")
                    raise

    def execute(
            self,
            query: Union[str, sql.Composable],
            params: Optional[tuple] = None,
            mapper: Optional[Callable[[Any], Dict]] = None,
            fetch: bool = True
    ) -> Union[List[Dict], int]:
        """
        Универсальный метод выполнения запросов

        Args:
            query: SQL-запрос или шаблон
            params: Параметры запроса
            mapper: Функция преобразования результата
            fetch: Флаг получения результатов

        Returns:
            Список результатов или количество затронутых строк
        """
        try:
            with self._get_cursor() as cur:
                cur.execute(query, params)

                if fetch and cur.description:
                    if mapper:
                        return [mapper(row) for row in cur.fetchall()]
                    return [dict(zip([col.name for col in cur.description], row))
                            for row in cur.fetchall()]

                return cur.rowcount

        except psycopg2.Error as e:
            logger.error(f"Query failed: {str(e)}")
            raise

    def get_companies_stats(self) -> List[Dict]:
        """Статистика по компаниям и вакансиям"""
        query = sql.SQL("""
            SELECT 
                e.employer_id,
                e.name AS company,
                COUNT(v.*) AS total_vacancies,
                AVG(v.salary_from)::numeric(10,2) AS avg_salary_from,
                AVG(v.salary_to)::numeric(10,2) AS avg_salary_to
            FROM employers e
            LEFT JOIN vacancies v USING(employer_id)
            GROUP BY e.employer_id
            HAVING COUNT(v.*) > 0
            ORDER BY total_vacancies DESC
        """)
        return self.execute(query)

    def get_vacancies(
            self,
            min_salary: Optional[int] = None,
            currency: str = 'RUR'
    ) -> List[Dict]:
        """Получение вакансий с фильтрацией"""
        base_query = sql.SQL("""
            SELECT 
                e.name AS company,
                v.title,
                v.salary_from,
                v.salary_to,
                v.currency,
                v.url,
                v.posted_at
            FROM vacancies v
            JOIN employers e USING(employer_id)
            WHERE currency = %s
        """)

        filters = [sql.SQL("currency = %s")]
        params: List[Any] = [currency]

        if min_salary:
            filters.append(sql.SQL("COALESCE(salary_from, salary_to) >= %s"))
            params.append(min_salary)

        query = sql.SQL(" ").join([
            base_query,
            sql.SQL("AND ").join(filters),
            sql.SQL("ORDER BY COALESCE(salary_from, salary_to) DESC")
        ])

        return self.execute(query, tuple(params))

    def get_salary_analytics(self) -> Dict[str, float]:
        """Расширенная аналитика зарплат"""
        query = sql.SQL("""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_from) AS median_from,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_to) AS median_to,
                MODE() WITHIN GROUP (ORDER BY currency) AS common_currency
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
        """)
        result = self.execute(query)
        return result[0] if result else {}

    def search_vacancies(
            self,
            keywords: List[str],
            full_text: bool = True
    ) -> List[Dict]:
        """Поиск вакансий с поддержкой полнотекстового поиска"""
        if full_text:
            query = sql.SQL("""
                SELECT 
                    title,
                    url,
                    salary_from,
                    salary_to,
                    ts_rank_cd(
                        to_tsvector('russian', title),
                        plainto_tsquery('russian', %s)
                    ) AS rank
                FROM vacancies
                WHERE to_tsvector('russian', title) @@ plainto_tsquery('russian', %s)
                ORDER BY rank DESC, salary_from DESC
            """)
            return self.execute(query, (' '.join(keywords), ' '.join(keywords)))

        # Простой LIKE-поиск
        query = sql.SQL("""
            SELECT title, url, salary_from, salary_to
            FROM vacancies
            WHERE title ILIKE ANY(%s)
            ORDER BY COALESCE(salary_from, salary_to) DESC
        """)
        patterns = [f'%{word}%' for word in keywords]
        return self.execute(query, (patterns,))

    def get_employer_vacancies(self, employer_id: str) -> List[Dict]:
        """Получение вакансий работодателя с пагинацией"""
        query = sql.SQL("""
            SELECT 
                title,
                salary_from,
                salary_to,
                currency,
                url,
                posted_at
            FROM vacancies
            WHERE employer_id = %s
            ORDER BY posted_at DESC
        """)
        return self.execute(query, (employer_id,))

    def close_pool(self) -> None:
        """Корректное закрытие всех соединений"""
        for conn in self._connection_pool:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
        self._connection_pool.clear()


if __name__ == "__main__":
    # Пример использования с обработкой ошибок
    from pprint import pprint

    try:
        manager = DBManager(pool_size=3)

        print("Топ компаний:")
        pprint(manager.get_companies_stats()[:2])

        print("\nВакансии с зарплатой от 100000 RUR:")
        pprint(manager.get_vacancies(min_salary=100000)[:1])

        print("\nАналитика зарплат:")
        pprint(manager.get_salary_analytics())

    except Exception as e:
        logger.error(f"Ошибка в работе менеджера БД: {str(e)}")
    finally:
        manager.close_pool()
