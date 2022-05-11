from contextlib import contextmanager

import psycopg2
import psycopg2.extensions
from dynaconf import settings
from src.utils import export_dynaconf_custom_settings


@contextmanager
def connect_postgres(env: str = 'development') -> psycopg2.extensions.connection:
    """Safety connect to Postgres.

    Keyword Arguments
    -----------------
        env `str`: Environment to load settings. (default = 'development')

    Returns
    -------
        `psycopg2.extensions.connection`: Returns a Postgres connection object.

    Example usage
    -------------
    >>> with connect_postgres(env = 'production') as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT version()')
            print(cursor.fetchone())
    ('PostgreSQL 14.2 (Debian 14.2-1.pgdg110+1)
    """

    export_dynaconf_custom_settings()
    env_settings = settings.from_env(env)

    connection = (
        psycopg2.connect(
            host = env_settings.HOST,
            database = env_settings.DATABASE,
            port = env_settings.PORT,
            user = env_settings.USERNAME,
            password = env_settings.PASSWORD
        )
    )

    try:
        yield connection

    finally:
        connection.close()
