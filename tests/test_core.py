from src.database.connection import connect_postgres
from src.utils import (GenerateFakeData, get_random_list_dict_item,
                       query_list_dict, spark_session)


def test_check_if_query_list_dict_function_returns_dict():
    my_dict = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]

    result = query_list_dict(
        list_dict = my_dict,
        key = 'id',
        value = 1
    )

    assert isinstance(result, dict)


def test_check_if_get_random_list_dict_item_function_returns_list():
    my_dict = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]

    result = get_random_list_dict_item(
        list_dict = my_dict
    )

    assert isinstance(result, list)


def test_check_if_get_random_list_dict_item_function_returns_dict_inside_list():
    my_dict = [{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}]

    result = get_random_list_dict_item(
        list_dict = my_dict
    )[0]

    assert isinstance(result, dict)


def test_check_if_get_random_list_dict_item_function_returns_TypeError_exception():
    my_dict = [{'id': 1, 'name': 'a'}, (1, 'b')]

    try:
        result = get_random_list_dict_item(
            list_dict = my_dict
        )
        response = False
    except TypeError:
        response = True

    assert response == True


def test_check_if_GenerateFakeData_generate_associado_data_returns_list():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_associado_data()

    assert isinstance(result, list)


def test_check_if_GenerateFakeData_generate_conta_data_returns_list():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_conta_data()

    assert isinstance(result, list)


def test_check_if_GenerateFakeData_generate_cartao_data_returns_list():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_cartao_data()

    assert isinstance(result, list)


def test_check_if_GenerateFakeData_generate_movimento_data_returns_list():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_movimento_data()

    assert isinstance(result, list)


def test_check_if_GenerateFakeData_n_equals_1_generate_associado_data_returns_list_len_1():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_associado_data()

    assert len(result) == 1


def test_check_if_GenerateFakeData_n_equals_1_generate_conta_data_returns_list_len_1():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_conta_data()

    assert len(result) == 1


def test_check_if_GenerateFakeData_n_equals_1_generate_cartao_data_returns_list_len_1():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_cartao_data()

    assert len(result) == 1


def test_check_if_GenerateFakeData_n_equals_1_generate_movimento_data_returns_list_len_1():
    fake = GenerateFakeData(n = 1)
    result = fake.generate_movimento_data()

    assert len(result) == 1


def test_check_if_spark_session_return_valid_SparkSession():
    with spark_session(app_name = 'my_app') as session:
        result = session.version

        if result:
            response = True

        if not result:
            response = False

        assert response == True


def test_check_if_connect_postgres_return_valid_connection():
    with connect_postgres(env = 'production') as connection:
        try:
            cursor = connection.cursor()
            cursor.execute('SELECT version()')
            result = cursor.fetchone()

            response = True
        except:
            response = False

        assert response == True
