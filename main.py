from transform import *
from load import *
from extract import *


#Todo Criar as Databases do Bigquery
#Todo Atualizar a Criacao Das Tabelas
#Todo Adequear a transformacao para os Dados que escolhemos(Pegar nos Slides as Novas Colunas)
#Todo Criar o Extract para os dados de Natalidade(Testar)
def etl_nascidos_vivos(file_key):
    dataset_name = "nascidos_vivos"
    data_folder_mortalidade = "DadosNascidosVivos"

    # Conexão com GCP
    client = gcp_connection(file_key)
    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client,dataset_name)
    # Verifica se as tabelas já existem, se não existe, cria
    table_nascidos_vivos = table_exist(client,dataset_fonte)

    df_nascidos_vivos = create_df_natalide(data_folder_mortalidade)

    # Incluir tabelas e dfs em uma biblioteca
    tables_dfs = {table_nascidos_vivos:df_nascidos_vivos}
    load_data(tables_dfs,client,dataset_fonte)

def etl_mortalidade(file_key):
    dataset_name = "mortalidade_infantil"
    data_folder_mortalidade = "DadosMortalidade"

    client = gcp_connection(file_key)
    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)
    # Verifica se as tabelas já existem, se não existe, cria
    table_mortalidade = table_exist(client, dataset_fonte)

    download_files(data_folder)

    df_mortalidade = create_df_mortalidade(data_folder_mortalidade)

    tables_dfs = {table_mortalidade: df_mortalidade}
    load_data(tables_dfs, client, dataset_fonte)


if __name__ == "__main__":
    file_key = "keys/ml-na-saude-ed1fc3c1a83e.json"
    etl_nascidos_vivos(file_key)
    etl_mortalidade(file_key)


