import itertools
from datetime import date
from pathlib import Path
from random import choice, randint, uniform
from typing import Dict, List, Union

import numpy
import pandas
import psycopg2
import psycopg2.extensions
from faker import Faker


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


def write_csv(data: List[Dict], output_file: str) -> None:
    """Writes data to CSV file.

    Arguments
    ---------
        data `List[Dict]`: Data to be writted to CSV.
        output_file `str`: Output CSV file.

    Example usage
    -------------
    >>> my_data = [{'id': 1, 'nome': 'Jamie', 'sobrenome': 'Smith'}]
    >>> write_csv(data = my_data, output_file = 'src/data/file.csv')
    """

    dataframe = pandas.DataFrame(data = data)
    dataframe.to_csv(
        Path(output_file),
        sep = ';',
        index = False
    )


def import_to_postgres(
    connection: psycopg2.extensions.connection,
    table: str,
    data: pandas.DataFrame
) -> None:

    cursor = connection.cursor()
    n_columns = data.shape[1]
    dataframe = data.replace({numpy.nan: None})
    dataframe_tuple = dataframe.to_records(index = False).tolist()

    values_placeholder = ','.join(['%s'] * n_columns)
    args_str = ','.join(cursor.mogrify(f"({values_placeholder})", row).decode('utf-8') for row in dataframe_tuple)
    cursor.execute(f"INSERT INTO {table} VALUES {args_str}")

    connection.commit()
