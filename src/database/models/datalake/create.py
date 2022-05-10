from src.database.connection import connect_postgres


def create_datalake(env: str = 'development') -> None:
    """Creates the `datalake` schema and the tables above:
    * associado
    * conta
    * cartao
    * movimento

    Keyword Arguments
    -----------------
        env `str`: Environment to load settings. (default = 'development')

    Example usage
    -------------
    >>> create_datalake(env = 'production')
    """

    with connect_postgres(env = env) as connection:
        cursor = connection.cursor()

        with open('src/database/models/datalake/create_schema.sql', mode = 'r') as file:
            query = file.read()
            cursor.execute(query)

        with open('src/database/models/datalake/associado.sql', mode = 'r') as file:
            query = file.read()
            cursor.execute(query)

        with open('src/database/models/datalake/conta.sql', mode = 'r') as file:
            query = file.read()
            cursor.execute(query)

        with open('src/database/models/datalake/cartao.sql', mode = 'r') as file:
            query = file.read()
            cursor.execute(query)

        with open('src/database/models/datalake/movimento.sql', mode = 'r') as file:
            query = file.read()
            cursor.execute(query)

        connection.commit()