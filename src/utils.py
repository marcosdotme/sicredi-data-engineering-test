import itertools
from os import environ
import pathlib
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from random import choice, randint, uniform
from typing import Dict, List, Optional, Union

import numpy
import psycopg2
import psycopg2.extensions
import pyspark
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_list_dict(list_dict: List[Dict], key: str, value: Union[str, int]) -> Dict:
    """Search for a value in an list of dictionaries and return the dictionary.

    If there's more than one match, return only the first match.

    Arguments
    ---------
        list_dict `List[Dict]`: Dictionary to search.
        key `str`: Key dictionary to search.
        value `Union[str, int]`: Key value to search.

    Returns
    -------
        `Dict`

    Example usage
    -------------
    >>> my_list = [{'id': 1, 'name': 'Chris'}, {'id': 2, 'name': 'Josh'}]
    >>> query_list_dict(list_dict = my_list, key = 'id', value = 1)
    {'id': 1, 'name': 'Chris'}
    """

    result = [item for item in list_dict if item[key] == value]
    return result[0]


def get_random_list_dict_item(list_dict: List[Dict]) -> List[Dict]:
    """Gets a random element from a list of dictionaries.

    Arguments
    ---------
        list_dict `List[Dict]`: List to get the random item.

    Raises
    ------
        `TypeError`: Raise exception if all elements in list are not `dict` type.

    Returns
    -------
        `List[Dict]`

    Example usage
    -------------
    >>> my_list = [{'id': 1, 'name': 'Chris'}, {'id': 2, 'name': 'Josh'}]
    >>> get_random_list_dict_item(list_dict = my_list)
    [{'id': 2, 'name': 'Josh'}]
    """

    if not all(isinstance(item, dict) for item in list_dict):
        raise TypeError(f'`list_dict` argument must be a list of dict.')

    return [choice(list_dict)]


class GenerateFakeData:
    """Generate fake data.
    
    Attributes
    ----------
        n `int`: Number of fake data rows to be generated. (default = 500)
        id `int`: Start number for the identifier of each fake data row. (default = 1)

    Methods
    -------
    `generate_associado_data()`:
        Generates fake data about `associado (people)`.

    `generate_conta_data()`:
        Generates fake data about `conta (bank account)`.

    `generate_cartao_data()`:
        Generates fake data about `cartao (credit card)`.

    `generate_movimento_data()`:
        Generates fake data about `movimento (financial transactions)`.

    Example usage
    -------------
    >>> fake = GenerateFakeData(n = 1, id = 1)
    >>> print(fake.generate_associado_data())
    [
        {
            'id': 1,
            'nome': 'Jamie',
            'sobrenome': 'Smith',
            'idade': 40,
            'email': 'gutierrezdavid@example.net'
        }
    ]
    """

    def __init__(self, n: int = 5, id: int = 1) -> None:
        self.n = n
        self.id = id
        self.associado_data = None
        self.cartao_data = None
        self.conta_data = None
        self.movimento_data = None


    def generate_associado_data(
        self
    ) -> List[Dict]:
        """Generates fake data about `associado (people)`.

        Returns
        -------
            `List[Dict]`: Returns fake data about `associado (people)`.

        Example usage
        -------------
        >>> fake = GenerateFakeData(n = 1, id = 1)
        >>> print(fake.generate_associado_data())
        [
            {
                'id': 1,
                'nome': 'Jamie',
                'sobrenome': 'Smith',
                'idade': 40,
                'email': 'gutierrezdavid@example.net'
            }
        ]
        """

        fake = Faker()
        fake_data = list()
        id = itertools.count(self.id)

        for _ in range(self.n):
            fake_row = {
                'id': next(id),
                'nome': fake.first_name(),
                'sobrenome': fake.last_name(),
                'idade': randint(20, 50),
                'email': fake.email()
            }

            fake_data.append(fake_row)

        self.associado_data = fake_data

        return fake_data


    def generate_conta_data(
        self
    ) -> List[Dict]:
        """Generates fake data about `conta (bank account)`.

        Returns
        -------
            `List[Dict]`: Returns fake data about `conta (bank account)`.

        Example usage
        -------------
        >>> fake = GenerateFakeData(n = 1, id = 1)
        >>> print(fake.generate_conta_data())
        [
            {
                'id': 1,
                'tipo': 'C/C',
                'data_criacao': datetime.datetime(1994, 9, 12, 21, 36, 30),
                'id_associado': 1
            }
        ]
        """

        fake = Faker()
        fake_data = list()
        id = itertools.count(self.id)
        tipo_conta = ['C/C', 'C/I']

        for _ in range(self.n):
            fake_row = {
                'id': next(id),
                'tipo': choice(tipo_conta),
                'data_criacao': fake.date_time(),
                'id_associado': randint(self.id, self.n)
            }

            fake_data.append(fake_row)

        self.conta_data = fake_data

        return fake_data


    def generate_cartao_data(
        self
    ) -> List[Dict]:
        """Generates fake data about `cartao (credit card)`.

        Returns
        -------
            `List[Dict]`: Returns fake data about `cartao (credit card)`.

        Example usage
        -------------
        >>> >>> fake = GenerateFakeData(n = 1, id = 1)
        >>> print(fake.generate_cartao_data())
        [
            {
                'id': 1,
                'num_cartao': '2227213156713788',
                'nom_impresso': 'Jamie Smith',
                'data_criacao': datetime.datetime(2001, 1, 12, 18, 36, 15),
                'id_conta': 1,
                'id_associado': 1
            }
        ]
        """

        fake = Faker()
        fake_data = list()
        id = itertools.count(self.id)

        if not self.conta_data:
            self.conta_data = self.generate_conta_data()

        if not self.associado_data:
            self.conta_data = self.generate_associado_data()

        for _ in range(self.n):
            _id = next(id)
            conta_data = query_list_dict(
                list_dict = self.conta_data,
                key = 'id',
                value = _id
            )

            # Filter `associado` data based on `conta id`
            # used to get the correct `nome` and `sobrenome`.
            associado_data = query_list_dict(
                list_dict = self.associado_data,
                key = 'id',
                value = conta_data['id_associado']
            )

            fake_row = {
                'id': _id,
                'num_cartao': fake.credit_card_number(card_type = 'mastercard'),
                'nom_impresso': f"{associado_data['nome']} {associado_data['sobrenome']}",
                'data_criacao': fake.date_time(),
                'id_conta': conta_data['id'],
                'id_associado': conta_data['id_associado']
            }

            fake_data.append(fake_row)

        self.cartao_data = fake_data

        return fake_data


    def generate_movimento_data(
        self
    ) -> List[Dict]:
        """Generates fake data about `movimento (financial transactions)`.

        Returns
        -------
            `List[Dict]`: Returns fake data about `movimento (financial transactions)`.

        Example usage
        -------------
        >>> fake = GenerateFakeData(n = 1, id = 1)
        >>> print(fake.generate_movimento_data())
        [
            {
                'id': 1,
                'vlr_transacao': 1383.09,
                'des_transacao': 'Ifood* Ifood',
                'data_movimento': datetime.datetime(2021, 7, 29, 17, 40, 51),
                'id_cartao': 1
            }
        ]
        """

        fake = Faker()
        fake_data = list()
        id = itertools.count(self.id)
        transaction_descriptions = [
            'Rappi* Rappi Brasil Int',
            'Uber* Uber*Trip',
            'Ifood* Ifood',
            'Mercpago* Meli',
            'Ame Digital',
            'Pag* Xpto',
            'Amazon',
            '99app *99app',
            'Paypal *Steam Games',
            'Mp* Xpto'
        ]

        if not self.cartao_data:
            self.cartao_data = self.generate_cartao_data()

        for _ in range(self.n):
            _id = next(id)

            cartao_data = get_random_list_dict_item(
                list_dict = self.cartao_data
            )[0]

            fake_row = {
                'id': _id,
                'vlr_transacao': round(uniform(10, 2000), 2),
                'des_transacao': choice(transaction_descriptions),
                'data_movimento': fake.date_time_between(
                    start_date = date(2021, 1, 1),
                    end_date = date(2022, 5, 1)),
                'id_cartao': cartao_data['id']
            }

            fake_data.append(fake_row)

        self.movimento_data = fake_data

        return fake_data


@contextmanager
def spark_session(app_name: str) -> SparkSession:
    """Safety creates a SparkSession.

    Arguments
    ---------
        app_name `str`: The name for the SparkSession.

    Returns
    -------
        `SparkSession`: Returns a SparkSession object.

    Example usage
    -------------
    >>> with spark_session(app_name = 'my_app') as session:
            session.version
    '3.2.1'
    """

    spark = (
        SparkSession.builder
            .master('local')
            .appName(app_name)
            .config('spark.jars', Path().cwd() / 'src/drivers/postgresql-42.3.5.jar')
            .getOrCreate()
    )

    try:
        yield spark

    finally:
        spark.stop()


def write_csv(data: List[Dict], output_dir: str, data_schema: Optional[pyspark.sql.types.StructType] = None) -> None:
    """Writes data to CSV file.

    Arguments
    ---------
        data `List[Dict]`: Data to be writted to CSV.
        output_dir `str`: Output directory to CSV file.

    Keyword Arguments
    -----------------
        data_schema `Optional[pyspark.sql.types.StructType]`: PySpark Schema for the DataFrame. (default = None)

    Example usage
    -------------
    >>> my_data = [{'id': 1, 'nome': 'Jamie', 'sobrenome': 'Smith'}]
    >>> write_csv(data = my_data, output_dir = 'src/data/')
    """
    
    with spark_session(app_name = 'app') as session:
        if data_schema:
            dataframe = session.createDataFrame(data, schema = data_schema)

        if not data_schema:
            dataframe = session.createDataFrame(data)

        (
            dataframe
                .write
                .csv(
                    output_dir,
                    sep = ';',
                    header = True,
                    mode = 'overwrite'
                )
        )


def import_to_postgres(
    connection: psycopg2.extensions.connection,
    table: str,
    data: DataFrame
) -> None:

    cursor = connection.cursor()

    n_columns = len(data.columns)
    dataframe = data.replace({numpy.nan: None})
    dataframe_tuple = dataframe.rdd.map(tuple).collect()

    values_placeholder = ','.join(['%s'] * n_columns)
    args_str = ','.join(cursor.mogrify(f"({values_placeholder})", row).decode('utf-8') for row in dataframe_tuple)
    
    cursor.execute(f"INSERT INTO {table} VALUES {args_str}")

    connection.commit()


def find_files(dir: str, file_extension: Optional[str] = None) -> List[pathlib.PosixPath]:
    """Find files in directory.

    Arguments
    ---------
        dir `str`: Directory to search files.

    Keyword Arguments
    -----------------
        file_extension `Optional[str]`: File extension to search. (default = None)

    Returns
    -------
        `List[pathlib.PosixPath]`: Returns a list with the files.

    Example usage
    -------------
    >>> find_files(dir = 'src/data/', file_extension = '.csv')
    [PosixPath('src/data/file_1.csv'), PosixPath('src/data/file_2.csv')]
    """

    if file_extension:
        extension = file_extension.split('.')[-1]

        return list(
            Path(dir).glob(f'*.{extension}')
        )

    if not file_extension:
        return list(
            Path(dir).glob('*')
        )


def delete_file(file: pathlib.PosixPath) -> None:
    """Deletes file.

    Arguments
    ---------
        file `pathlib.PosixPath`: File to be deleted.

    Example usage
    -------------
    >>> delete_file(file = 'src/data/file_1.csv')
    """

    Path(file).unlink(missing_ok = True)


def export_dynaconf_custom_settings(
    secrets_toml: str = '.secrets/.secrets.toml',
    settings_toml: str = '.secrets/settings.toml'
) -> None:
    """Exports custom files to Dynaconf.
    
    By exporting values to `SETTINGS_FILE_FOR_DYNACONF`
    and `SECRETS_FOR_DYNACONF`, Dynaconf can read an we can
    use this values as normal.

    Keyword Arguments
    -----------------
        secrets_toml `str`: Path to `.secrets.toml` file. (default = '.secrets/.secrets.toml')
        settings_toml `str`: Path to `settings.toml` file. (default = '.secrets/settings.toml')

    Example usage
    -------------
    >>> export_dynaconf_custom_settings()
    """

    environ.setdefault('SETTINGS_FILE_FOR_DYNACONF', secrets_toml)
    environ.setdefault('SECRETS_FOR_DYNACONF', settings_toml)


def drop_table(
    connection: psycopg2.extensions.connection,
    schema: str,
    table: str
) -> None:
    """Drops table if exists in schema.

    Arguments
    ---------
        connection `psycopg2.extensions.connection`: Connection to database.
        schema `str`: Database schema name.
        table `str`: Table name.

    Example usage
    -------------
    >>> drop_table(connection = connection, schema = 'db_schema', table = 'customers')
    """
    
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

    connection.commit()
