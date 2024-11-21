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

def table_exist_natalidade(client,dataset_fonte):
    ##########################################################################
    #                    Cria as tabelas caso não existam                    #
    ##########################################################################

    # Tabela e schema da table_nascidos_vivos
    table_nascidos_vivos = dataset_fonte.table("nascidos_vivos")

    schema_nascidos_vivos = [
        bigquery.SchemaField("cd_mun_res", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ano", "STRING", mode="REQUIRED"),
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

def table_exist_mortalidade(client, dataset_fonte):
    ##########################################################################
    #                    Cria as tabelas caso não existam                    #
    ##########################################################################

    # Tabela e schema da table_mortalidade
    table_mortalidade = dataset_fonte.table("mortalidade_infantil")

    schema_mortalidade = [
        bigquery.SchemaField("ano", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mes", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dt_obito", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("dt_nasc", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("cd_mun_res", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("SEXO", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("RACACOR", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ESCMAE", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("CAUSABAS", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("numero_obitos", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("taxa_mortaliadade", 'FLOAT', mode="REQUIRED"),
        bigquery.SchemaField("total_nascidos", 'INTEGER', mode="REQUIRED")
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

# Nova função para verificar e criar a tabela dim_tempo
def table_exist_tempo(client, dataset_fonte):
    table_tempo = dataset_fonte.table("dim_tempo")

    schema_tempo = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ano", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("mes", "INTEGER", mode="REQUIRED"),
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência da tabela de dimensão tempo no GCP...")
    try:
        client.get_table(table_tempo, timeout=30)
        print(f"A tabela {table_tempo} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_tempo} não encontrada! Criando tabela {table_tempo}...")
        client.create_table(bigquery.Table(table_tempo, schema=schema_tempo))
        print(f"A tabela {table_tempo} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_tempo

# Nova função para verificar e criar a tabela dim_raca
def table_exist_raca(client, dataset_fonte):
    table_raca = dataset_fonte.table("dim_raca")

    schema_raca = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("raca", "STRING", mode="REQUIRED"),
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência da tabela de dimensão raca no GCP...")
    try:
        client.get_table(table_raca, timeout=30)
        print(f"A tabela {table_raca} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_raca} não encontrada! Criando tabela {table_raca}...")
        client.create_table(bigquery.Table(table_raca, schema=schema_raca))
        print(f"A tabela {table_raca} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_raca

# Nova função para verificar e criar a tabela dim_sexo
def table_exist_sexo(client, dataset_fonte):
    table_sexo = dataset_fonte.table("dim_sexo")

    schema_sexo = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("sexo", "STRING", mode="REQUIRED"),
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência da tabela de dimensão sexo no GCP...")
    try:
        client.get_table(table_sexo, timeout=30)
        print(f"A tabela {table_sexo} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_sexo} não encontrada! Criando tabela {table_sexo}...")
        client.create_table(bigquery.Table(table_sexo, schema=schema_sexo))
        print(f"A tabela {table_sexo} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_sexo

# Nova função para verificar e criar a tabela dim_municipio
def table_exist_municipio(client, dataset_fonte):
    table_municipio = dataset_fonte.table("dim_municipio")

    schema_municipio = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ibge", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("uf", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("municipio", "STRING", mode="REQUIRED"),
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência da tabela de dimensão município no GCP...")
    try:
        client.get_table(table_municipio, timeout=30)
        print(f"A tabela {table_municipio} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_municipio} não encontrada! Criando tabela {table_municipio}...")
        client.create_table(bigquery.Table(table_municipio, schema=schema_municipio))
        print(f"A tabela {table_municipio} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_municipio

# Nova função para verificar e criar a tabela dim_escolaridade_mae
def table_exist_escolaridade_mae(client, dataset_fonte):
    table_escolaridade_mae = dataset_fonte.table("dim_escolaridade_mae")

    schema_escolaridade_mae = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("descricao", "STRING", mode="REQUIRED"),
    ]

    print("--------------------------------------------------------------------------")
    print("Verificando a existência da tabela de dimensão escolaridade_mae no GCP...")
    try:
        client.get_table(table_escolaridade_mae, timeout=30)
        print(f"A tabela {table_escolaridade_mae} já existe!")
        print("--------------------------------------------------------------------------")
    except:
        print(f"Tabela {table_escolaridade_mae} não encontrada! Criando tabela {table_escolaridade_mae}...")
        client.create_table(bigquery.Table(table_escolaridade_mae, schema=schema_escolaridade_mae))
        print(f"A tabela {table_escolaridade_mae} foi criada.")
        print("--------------------------------------------------------------------------")

    return table_escolaridade_mae

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