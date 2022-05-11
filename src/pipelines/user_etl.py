from dynaconf import settings
from src.utils import export_dynaconf_custom_settings, spark_session


def user_etl(output_dir: str) -> None:
    """Writes a CSV file on directory

    Arguments
    ---------
        output_dir `str`: The directory to write the CSV file.

    Example usage
    -------------
    >>> user_etl(output_dir = 'src/data/')
    """

    with spark_session(app_name = 'User_ETL') as session:
        export_dynaconf_custom_settings()
        env_settings = settings.from_env('production')

        query = """
        SELECT DISTINCT
            T1.nome 			AS nome_associado,
            T1.sobrenome 		AS sobrenome_associado,
            T1.idade 			AS idade_associado,
            T4.vlr_transacao 	AS vlr_transacao_movimento,
            T4.des_transacao 	AS des_transacao_movimento,
            T4.data_movimento	AS data_movimento,
            T3.num_cartao 		AS numero_cartao,
            T3.nom_impresso 	AS nome_impresso_cartao,
            T3.data_criacao		AS data_criacao_cartao,
            T2.tipo 			AS tipo_conta,
            T2.data_criacao 	AS data_criacao_conta
        FROM
            datalake.associado AS T1
            INNER JOIN datalake.conta AS T2 ON T2.id_associado = T1.id
            INNER JOIN datalake.cartao AS T3 ON T3.id_associado = T1.id
            INNER JOIN datalake.movimento AS T4 ON T4.id_cartao = T3.id
        """

        dataframe = (
            session
                .read.format('jdbc')
                .options(
                    url = f'jdbc:postgresql://{env_settings.HOST}:{env_settings.PORT}/{env_settings.DATABASE}',
                    dbtable = f'({query}) AS flat_table',
                    user = env_settings.USERNAME,
                    password = env_settings.PASSWORD,
                    driver = 'org.postgresql.Driver')
                .load()
        )

        # Write
        (
            dataframe
                .write
                .csv(
                    str(output_dir),
                    sep = ';',
                    header = True,
                    mode = 'overwrite'
                )
        )
