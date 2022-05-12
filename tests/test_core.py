from pathlib import Path
from random import randrange
from tempfile import TemporaryDirectory

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.database.connection import connect_postgres
from src.pipelines.prepare import prepare_environment
from src.pipelines.user_etl import user_etl
from src.utils import (GenerateFakeData, delete_file, find_files,
                       get_random_list_dict_item, query_list_dict,
                       spark_session, write_csv)


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

    with pytest.raises(TypeError):
        result = get_random_list_dict_item(
            list_dict = my_dict
        )


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
        spark_version = session.version

        if spark_version:
            result = True

        if not spark_version:
            result = False

        assert result == True


def test_check_if_write_csv_can_writes_csv_file_with_schema_option():
    my_data = [{'id': 1, 'nome': 'Jamie', 'sobrenome': 'Smith'}]
    data_schema = StructType(
        [
            StructField(name = 'id', dataType = IntegerType()),
            StructField(name = 'nome', dataType = StringType()),
            StructField(name = 'sobrenome', dataType = StringType())
        ]
    )

    with TemporaryDirectory() as temp_dir:
        write_csv(
            data = my_data,
            output_dir = temp_dir,
            data_schema = data_schema
        )

        files = find_files(
            dir = temp_dir,
            file_extension = '.csv'
        )

        assert len(files) > 0


def test_check_if_write_csv_can_writes_csv_file_without_schema_option():
    my_data = [{'id': 1, 'nome': 'Jamie', 'sobrenome': 'Smith'}]

    with TemporaryDirectory() as temp_dir:
        write_csv(
            data = my_data,
            output_dir = temp_dir
        )

        files = find_files(
            dir = temp_dir,
            file_extension = '.csv'
        )

        assert len(files) > 0


def test_check_if_find_files_can_find_all_files_in_directory():
    files_to_find = [
        'file_1.csv',
        'file_2.txt',
        'file_3.json',
        'file_4.csv',
        'file_5.txt',
        'file_6.json',
        'file_7.csv'
    ]

    with TemporaryDirectory() as temp_dir:
        for file in files_to_find:
            file_in_temp_dir = Path(temp_dir) / file
            
            with file_in_temp_dir.open(mode = 'w') as file:
                file.write('This is a temporary file.')

        files_founded = find_files(
            dir = temp_dir
        )
        
        assert len(files_founded) == 7


def test_check_if_find_files_can_find_all_csv_files_in_directory():
    files_to_find = [
        'file_1.csv',
        'file_2.txt',
        'file_3.json',
        'file_4.csv',
        'file_5.txt',
        'file_6.json',
        'file_7.csv'
    ]

    with TemporaryDirectory() as temp_dir:
        for file in files_to_find:
            file_in_temp_dir = Path(temp_dir) / file
            
            with file_in_temp_dir.open(mode = 'w') as file:
                file.write('This is a temporary file.')

        files_founded = find_files(
            dir = temp_dir,
            file_extension = '.csv'
        )
        
        assert len(files_founded) == 3


def test_check_if_delete_file_deletes_file():
    files_to_create = [
        'file_1.csv',
        'file_2.txt',
        'file_3.json',
        'file_4.csv',
        'file_5.txt',
        'file_6.json',
        'file_7.csv'
    ]

    with TemporaryDirectory() as temp_dir:
        for file in files_to_create:
            file_in_temp_dir = Path(temp_dir) / file
            
            with file_in_temp_dir.open(mode = 'w') as file:
                file.write('This is a temporary file.')

        files_founded = find_files(
            dir = temp_dir
        )

        random_file_index = randrange(0, len(files_to_create))
        file_to_delete = files_founded[random_file_index]

        delete_file(file = file_to_delete)

        assert file_to_delete.exists() == False


def test_check_if_connect_postgres_return_valid_connection():
    with connect_postgres(env = 'production') as connection:
        try:
            cursor = connection.cursor()
            cursor.execute('SELECT version()')
            postgres_version = cursor.fetchone()

            result = True
        except:
            result = False

        assert result == True


def test_if_user_etl_successfully_executes_and_writes_the_csv_file():
    with TemporaryDirectory() as temp_dir:

        user_etl(output_dir = temp_dir)

        files = find_files(
            dir = temp_dir,
            file_extension = '.csv'
        )

        assert len(files) > 0


def test_if_prepare_environment_successfully_creates_schema():
    prepare_environment()

    query = """
    SELECT
        EXISTS (
                    SELECT
                        schema_name
                    FROM
                        information_schema.schemata
                    WHERE
                        schema_name = 'datalake'
                );
    """

    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]

        assert result == True


def test_if_prepare_environment_successfully_creates_associado_table():
    query = """
    SELECT
        EXISTS (
                    SELECT
                        table_name
                    FROM
                        information_schema.tables
                    WHERE
                        table_schema = 'datalake'
                        AND table_name = 'associado'
                );
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]

        assert result == True


def test_if_prepare_environment_successfully_creates_conta_table():
    query = """
    SELECT
        EXISTS (
                    SELECT
                        table_name
                    FROM
                        information_schema.tables
                    WHERE
                        table_schema = 'datalake'
                        AND table_name = 'conta'
                );
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]

        assert result == True


def test_if_prepare_environment_successfully_creates_cartao_table():
    query = """
    SELECT
        EXISTS (
                    SELECT
                        table_name
                    FROM
                        information_schema.tables
                    WHERE
                        table_schema = 'datalake'
                        AND table_name = 'cartao'
                );
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]

        assert result == True


def test_if_prepare_environment_successfully_creates_movimento_table():
    query = """
    SELECT
        EXISTS (
                    SELECT
                        table_name
                    FROM
                        information_schema.tables
                    WHERE
                        table_schema = 'datalake'
                        AND table_name = 'movimento'
                );
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]

        assert result == True


def test_if_prepare_environment_successfully_populates_associado_table():
    query = """
    SELECT
        COUNT(*)
    FROM
        datalake.associado
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]

        assert count > 0


def test_if_prepare_environment_successfully_populates_conta_table():
    query = """
    SELECT
        COUNT(*)
    FROM
        datalake.conta
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]

        assert count > 0


def test_if_prepare_environment_successfully_populates_cartao_table():
    query = """
    SELECT
        COUNT(*)
    FROM
        datalake.cartao
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]

        assert count > 0


def test_if_prepare_environment_successfully_populates_movimento_table():
    query = """
    SELECT
        COUNT(*)
    FROM
        datalake.movimento
    """
    
    with connect_postgres(env = 'production') as connection:
        cursor = connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]

        assert count > 0
