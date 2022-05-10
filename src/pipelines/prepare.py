from pyspark.sql.types import (FloatType, IntegerType, StringType,
                               StructField, StructType, TimestampType)
from src.database.connection import connect_postgres
from src.database.models.datalake.create import create_datalake
from src.utils import (GenerateFakeData, delete_file, find_files,
                       import_to_postgres, spark_session, write_csv)



def prepare_environment() -> None:
    """This function will create the environment by doing this steps:

    * Creates Postgres schema;
    * Creates Postgres tables;
    * Generates fake data;
    * Imports fake data to Postgres tables;

    Example usage
    -------------
    >>> prepare_environment()
    """

    # Creates 'datalake' schema and tables on Postgres
    create_datalake(env = 'production')


    # Tables schemas for PySpark
    associado_data_schema = StructType(
        [
            StructField(name = 'id', dataType = IntegerType()),
            StructField(name = 'nome', dataType = StringType()),
            StructField(name = 'sobrenome', dataType = StringType()),
            StructField(name = 'idade', dataType = IntegerType()),
            StructField(name = 'email', dataType = StringType())
        ]
    )

    conta_data_schema = StructType(
        [
            StructField(name = 'id', dataType = IntegerType()),
            StructField(name = 'tipo', dataType = StringType()),
            StructField(name = 'data_criacao', dataType = TimestampType()),
            StructField(name = 'id_associado', dataType = IntegerType())
        ]
    )

    cartao_data_schema = StructType(
        [
            StructField(name = 'id', dataType = IntegerType()),
            StructField(name = 'num_cartao', dataType = StringType()),
            StructField(name = 'nom_impresso', dataType = StringType()),
            StructField(name = 'data_criacao', dataType = TimestampType()),
            StructField(name = 'id_conta', dataType = IntegerType()),
            StructField(name = 'id_associado', dataType = IntegerType())
        ]
    )

    movimento_data_schema = StructType(
        [
            StructField(name = 'id', dataType = IntegerType()),
            StructField(name = 'vlr_transacao', dataType = FloatType()),
            StructField(name = 'des_transacao', dataType = StringType()),
            StructField(name = 'data_movimento', dataType = TimestampType()),
            StructField(name = 'id_cartao', dataType = IntegerType())
        ]
    )


    # Generates fake data
    fake = GenerateFakeData(n = 2000, id = 1)

    associado_data = fake.generate_associado_data()
    conta_data = fake.generate_conta_data()
    cartao_data = fake.generate_cartao_data()
    movimento_data = fake.generate_movimento_data()


    write_csv(
        data = associado_data,
        output_dir = 'src/data/associado/',
        data_schema = associado_data_schema
    )

    write_csv(
        data = conta_data,
        output_dir = 'src/data/conta/',
        data_schema = conta_data_schema
    )

    write_csv(
        data = cartao_data,
        output_dir = 'src/data/cartao/',
        data_schema = cartao_data_schema
    )

    write_csv(
        data = movimento_data,
        output_dir = 'src/data/movimento/',
        data_schema = movimento_data_schema
    )


    # Imports fake data for Postgres
    with connect_postgres(env = 'production') as connection:

        with spark_session(app_name = 'ETL') as session:

            associado_files = find_files(
                dir = 'src/data/associado/',
                file_extension = '.csv'
            )

            conta_files = find_files(
                dir = 'src/data/conta/',
                file_extension = '.csv'
            )

            cartao_files = find_files(
                dir = 'src/data/cartao/',
                file_extension = '.csv'
            )

            movimento_files = find_files(
                dir = 'src/data/movimento/',
                file_extension = '.csv'
            )


            for file in associado_files:
                associado_data = (
                    session
                        .read
                        .format('csv')
                        .option('sep', ';')
                        .option('header', True)
                        .schema(associado_data_schema)
                        .load(str(file))
                )

                import_to_postgres(
                    connection = connection,
                    table = 'datalake.associado',
                    data = associado_data
                )
                delete_file(file = file)


            for file in conta_files:
                conta_data = (
                    session
                        .read
                        .format('csv')
                        .option('sep', ';')
                        .option('header', True)
                        .schema(conta_data_schema)
                        .load(str(file))
                )

                import_to_postgres(
                    connection = connection,
                    table = 'datalake.conta',
                    data = conta_data
                )
                delete_file(file = file)


            for file in cartao_files:
                cartao_data = (
                    session
                        .read
                        .format('csv')
                        .option('sep', ';')
                        .option('header', True)
                        .schema(cartao_data_schema)
                        .load(str(file))
                )

                import_to_postgres(
                    connection = connection,
                    table = 'datalake.cartao',
                    data = cartao_data
                )
                delete_file(file = file)

            
            for file in movimento_files:
                movimento_data = (
                    session
                        .read
                        .format('csv')
                        .option('sep', ';')
                        .option('header', True)
                        .schema(movimento_data_schema)
                        .load(str(file))
                )

                import_to_postgres(
                    connection = connection,
                    table = 'datalake.movimento',
                    data = movimento_data
                )
                delete_file(file = file)
