# importar bibliotecas
from google.cloud import bigquery
from google.oauth2 import service_account
import os


def gcp_connection(file_key):
    ##########################################################################
    #                        Cria a conexão com o GCP                        #
    ##########################################################################

    print("##########################################################################")
    print("#                     Iniciando execução do programa                     #")
    print("##########################################################################")
    print("--------------------------------------------------------------------------")
    print("Criando conexão com o GCP...")
    try:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_directory, file_key)
        credentials = service_account.Credentials.from_service_account_file(file_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"Conexão realizada com sucesso com o projeto {credentials.project_id}.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Não foi possível efetivar a conexão com o GCP.")
        print("--------------------------------------------------------------------------")
    return client

def dataset_exist(client,dataset_name):
    ##########################################################################
    #                     Cria o dataset caso não exista                     #
    ##########################################################################
    print("--------------------------------------------------------------------------")
    print("Verificando a existência do dataset no GCP...")
    dataset_fonte = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} já existe no GCP.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Dataset {dataset_fonte} não foi encontrado no GCP, criando o dataset...")
        client.create_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} foi criado no GCP com sucesso.")
        print("--------------------------------------------------------------------------")
    return dataset_fonte

def table_exist(client,dataset_fonte):
    ##########################################################################
    #                    Cria as tabelas caso não existam                    #
    ##########################################################################

    # Tabela e schema da table_nascidos_vivos
    table_nascidos_vivos = dataset_fonte.table("nascidos_vivos")

    schema_nascidos_vivos = [
        bigquery.SchemaField("cd_mun_res", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ano", "STRING", mode="REQUIRED"),
        #bigquery.SchemaField("quad", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mes", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("total_nascidos", "FLOAT", mode="REQUIRED"),
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência das tabelas no GCP...")
    try:
        client.get_table(table_nascidos_vivos, timeout=30)
        print(f"A tabela {table_nascidos_vivos} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_nascidos_vivos} não encontrada! Criando tabela {table_nascidos_vivos}...")
        client.create_table(bigquery.Table(table_nascidos_vivos, schema=schema_nascidos_vivos))
        print(f"A tabela {table_nascidos_vivos} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_nascidos_vivos

def load_data(tables_dfs,client,dataset_fonte):
    print("--------------------------------------------------------------------------")
    print("Carregando dados no GCP...")
    for tabela, df in tables_dfs.items():
        table_ref = client.dataset(dataset_fonte.dataset_id).table(tabela.table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Dados carregados na tabela {tabela}.")

    print("--------------------------------------------------------------------------")
    print("##########################################################################")
    print("#                         Dados carregados no GCP                        #")
    print("##########################################################################")

    def gcp_connection(file_key):
        ##########################################################################
        #                        Cria a conexão com o GCP                        #
        ##########################################################################

        print("##########################################################################")
        print("#                     Iniciando execução do programa                     #")
        print("##########################################################################")
        print("--------------------------------------------------------------------------")
        print("Criando conexão com o GCP...")
        try:
            current_directory = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(current_directory, file_key)
            credentials = service_account.Credentials.from_service_account_file(file_path)
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)
            print(f"Conexão realizada com sucesso com o projeto {credentials.project_id}.")
            print("--------------------------------------------------------------------------")
        except Exception:
            print(f"Não foi possível efetivar a conexão com o GCP.")
            print("--------------------------------------------------------------------------")
        return client

    def dataset_exist(client, dataset_name):
        ##########################################################################
        #                     Cria o dataset caso não exista                     #
        ##########################################################################
        print("--------------------------------------------------------------------------")
        print("Verificando a existência do dataset no GCP...")
        dataset_fonte = client.dataset(dataset_name)
        try:
            client.get_dataset(dataset_fonte)
            print(f"O conjunto de dados {dataset_fonte} já existe no GCP.")
            print("--------------------------------------------------------------------------")
        except Exception:
            print(f"Dataset {dataset_fonte} não foi encontrado no GCP, criando o dataset...")
            client.create_dataset(dataset_fonte)
            print(f"O conjunto de dados {dataset_fonte} foi criado no GCP com sucesso.")
            print("--------------------------------------------------------------------------")
        return dataset_fonte

    def table_exist(client, dataset_fonte):
        ##########################################################################
        #                    Cria as tabelas caso não existam                    #
        ##########################################################################

        # Tabela e schema da table_mortalidade
        table_mortalidade = dataset_fonte.table("mortalidade_infantil")

        schema_mortalidade = [
            bigquery.SchemaField("ano_obito", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quad", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("dt_obito", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("dt_nasc", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("idade", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("cd_mun_res", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("populacao", "INTEGER", mode="REQUIRED")
        ]

        print("--------------------------------------------------------------------------")
        print("Verificando a existência das tabelas no GCP...")
        try:
            client.get_table(table_mortalidade, timeout=30)
            print(f"A tabela {table_mortalidade} já existe!")
            print("--------------------------------------------------------------------------")
        except:
            print(f"Tabela {table_mortalidade} não encontrada! Criando tabela {table_mortalidade}...")
            client.create_table(bigquery.Table(table_mortalidade, schema=schema_mortalidade))
            print(f"A tabela {table_mortalidade} foi criada.")
            print("--------------------------------------------------------------------------")

        return table_mortalidade

    def load_data(tables_dfs, client, dataset_fonte):
        print("--------------------------------------------------------------------------")
        print("Carregando dados no GCP...")
        for tabela, df in tables_dfs.items():
            table_ref = client.dataset(dataset_fonte.dataset_id).table(tabela.table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"Dados carregados na tabela {tabela}.")

        print("--------------------------------------------------------------------------")
        print("##########################################################################")
        print("#                         Dados carregados no GCP                        #")
        print("##########################################################################")