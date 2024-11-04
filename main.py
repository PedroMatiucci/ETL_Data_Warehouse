from transform import *
from load import *
from extract import *

def etl_nascidos_vivos(file_key):
    file_key = "keys/ml-na-saude-ed1fc3c1a83e.json"
    dataset_name = "nascidos_vivos"
    data_folder = "DadosNascidosVivos"

    # Conexão com GCP
    client = gcp_connection(file_key)
    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client,dataset_name)
    # Verifica se as tabelas já existem, se não existe, cria
    table_nascidos_vivos = table_exist(client,dataset_fonte)

    df_nascidos_vivos = create_df_natalide(data_folder)

    # Incluir tabelas e dfs em uma biblioteca
    tables_dfs = {table_nascidos_vivos:df_nascidos_vivos}
    load_data(tables_dfs,client,dataset_fonte)

def etl_mortalidade(file_key):
    file_key = "keys/ml-na-saude-ed1fc3c1a83e.json"
    dataset_name = "mortalidade_infantil"
    data_folder = r"C:\Users\User\Documents\GitHub\machine_learning_na_saude\pipelines\mortalidade_infantil\dados"

    client = gcp_connection(file_key)
    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)
    # Verifica se as tabelas já existem, se não existe, cria
    table_mortalidade = table_exist(client, dataset_fonte)

    download_files(data_folder)

    df_mortalidade = create_df_mortalidade(data_folder)

    tables_dfs = {table_mortalidade: df_mortalidade}
    load_data(tables_dfs, client, dataset_fonte)


if __name__ == "__main__":
    file_key = "keys/ml-na-saude-ed1fc3c1a83e.json"
    etl_nascidos_vivos(file_key)
    etl_mortalidade(file_key)


