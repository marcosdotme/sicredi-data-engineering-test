import psycopg2
import psycopg2.extensions
from contextlib import contextmanager


@contextmanager
def connect_postgres() -> psycopg2.extensions.connection:
    connection = (
        psycopg2.connect(
            host = 'localhost',
            database = 'sicooperative',
            port = '5432',
            user = 'root',
            password = 'root'
        )
    )

    try:
        yield connection

    finally:
        connection.close()
