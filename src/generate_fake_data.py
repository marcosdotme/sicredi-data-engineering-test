import itertools
from random import choice, randint, uniform
from typing import Dict, List

import pandas
from faker import Faker

from utils import get_random_list_dict_item, query_list_dict


class GenerateFakeData():
    """<Insert function description>."""

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

        Keyword Arguments
        -----------------
            n `int`: Number of fake data rows to be generated. (default = 500)
            id `int`: Start number for the identifier of each fake data row. (default = 1)

        Returns
        -------
            `List[Dict]`: Returns fake data.

        Example usage
        -------------
        >>> generate_associado_data(n = 1, id = 357)
        [{'id': 357, 'name': 'Ashley', 'sobrenome': 'Wilson', 'idade': 49, 'email': 'wnewton@example.net'}]
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

        Keyword Arguments
        -----------------
            n `int`: Number of fake data rows to be generated. (default = 500)
            id `int`: Start number for the identifier of each fake data row. (default = 1)

        Returns
        -------
            `List[Dict]`: Returns fake data.

        Example usage
        -------------
        >>> generate_conta_data(n = 1, id = 357)
        []
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

        Keyword Arguments
        -----------------
            n `int`: Number of fake data rows to be generated. (default = 500)
            id `int`: Start number for the identifier of each fake data row. (default = 1)

        Returns
        -------
            `List[Dict]`: Returns fake data.

        Example usage
        -------------
        >>> generate_cartao_data(n = 1, id = 357)
        []
        """
        fake = Faker()
        fake_data = list()
        id = itertools.count(self.id)

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

        Keyword Arguments
        -----------------
            n `int`: Number of fake data rows to be generated. (default = 500)
            id `int`: Start number for the identifier of each fake data row. (default = 1)

        Returns
        -------
            `List[Dict]`: Returns fake data.

        Example usage
        -------------
        >>> generate_movimento_data(n = 1, id = 357)
        []
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

        for _ in range(self.n):
            _id = next(id)

            cartao_data = get_random_list_dict_item(
                list_dict = self.cartao_data
            )

            fake_row = {
                'id': _id,
                'vlr_transacao': round(uniform(10, 2000), 2),
                'des_transacao': choice(transaction_descriptions),
                'data_movimento': fake.date_this_year(),
                'id_cartao': cartao_data['id']
            }

            fake_data.append(fake_row)

        self.movimento_data = fake_data

        return fake_data


_fake = GenerateFakeData()
df = pandas.DataFrame(_fake.generate_associado_data())
print('============================== ASSOCIADO ==============================')
print(df)
print('=======================================================================')

df = pandas.DataFrame(_fake.generate_conta_data())
print('============================== CONTA ==================================')
print(df)
print('=======================================================================')

df = pandas.DataFrame(_fake.generate_cartao_data())
print('============================== CARTÃO =================================')
print(df)
print('=======================================================================')

df = pandas.DataFrame(_fake.generate_movimento_data())
print('============================== MOVIMENTO ==============================')
print(df)
print('=======================================================================')
