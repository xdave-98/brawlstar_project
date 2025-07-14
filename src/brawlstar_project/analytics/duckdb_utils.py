from functools import wraps

import duckdb

from brawlstar_project.constants.paths import get_data_root


def duckdb_query(func):
    """
    Decorator to handle DuckDB connection management. The decorated function should accept a DuckDB connection as its first argument and return the result (usually a DataFrame).
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        con = duckdb.connect()
        try:
            result = func(con, *args, **kwargs)
        finally:
            con.close()
        return result

    return wrapper


def duckdb_simple_query(path_key="fact_matches.parquet"):
    """
    Decorator for simple DuckDB queries that only need a path and return a query string.
    The decorated function should accept 'path' as its first argument and return a SQL query string.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            data_root = get_data_root()
            path = data_root / path_key
            con = duckdb.connect()
            try:
                query = func(path, *args, **kwargs)
                df = con.execute(query).df()
            finally:
                con.close()
            return df

        return wrapper

    return decorator
