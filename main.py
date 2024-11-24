from transform import *
from load import *
from extract import *


def etl_nascidos_vivos(file_key):
    dataset_name = "mortalidade_infantil"
    data_folder_mortalidade = "DadosNascidosVivos"

    # Conexão com GCP
    client = gcp_connection(file_key)
    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client,dataset_name)
    # Verifica se as tabelas já existem, se não existe, cria
    table_nascidos_vivos = table_exist_natalidade(client,dataset_fonte)

    download_files_natalidade(data_folder_mortalidade)

    df_nascidos_vivos = create_df_natalidade(data_folder_mortalidade)

    # Incluir tabelas e dfs em uma biblioteca
    tables_dfs = {table_nascidos_vivos:df_nascidos_vivos}
    load_data(tables_dfs,client,dataset_fonte)
    return df_nascidos_vivos

def etl_mortalidade(file_key, df_natalidade):
    dataset_name = "mortalidade_infantil"
    data_folder_mortalidade = "DadosMortalidade"

    client = gcp_connection(file_key)
    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)
    # Verifica se as tabelas já existem, se não existe, cria
    table_mortalidade = table_exist_mortalidade(client, dataset_fonte)

    download_files_mortalidade(data_folder_mortalidade)

    df_mortalidade = create_df_mortalidade(data_folder_mortalidade, client, dataset_fonte, df_natalidade)

    tables_dfs = {table_mortalidade: df_mortalidade}
    load_data(tables_dfs, client, dataset_fonte)

def etl_tempo(file_key):
    dataset_name = "mortalidade_infantil"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela de tempo já existe, se não, cria
    table_tempo = table_exist_tempo(client, dataset_fonte)

    # Gera o dataframe da dimensão tempo
    df_tempo = create_df_tempo()

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_tempo: df_tempo}
    load_data(tables_dfs, client, dataset_fonte)

# Nova função para carregar a dimensão raca
def etl_raca(file_key):
    dataset_name = "mortalidade_infantil"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela de raca já existe, se não, cria
    table_raca = table_exist_raca(client, dataset_fonte)

    # Gera o dataframe da dimensão raca
    df_raca = create_df_raca()

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_raca: df_raca}
    load_data(tables_dfs, client, dataset_fonte)

# Nova função para carregar a dimensão sexo
def etl_sexo(file_key):
    dataset_name = "mortalidade_infantil"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela de sexo já existe, se não, cria
    table_sexo = table_exist_sexo(client, dataset_fonte)

    # Gera o dataframe da dimensão sexo
    df_sexo = create_df_sexo()

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_sexo: df_sexo}
    load_data(tables_dfs, client, dataset_fonte)

# Nova função para carregar a dimensão municipio
def etl_municipio(file_key):
    dataset_name = "mortalidade_infantil"
    data_folder_municipio = "DadosMunicipio"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela de município já existe, se não, cria
    table_municipio = table_exist_municipio(client, dataset_fonte)

    # Gera o dataframe da dimensão município
    df_municipio = create_df_municipio(data_folder_municipio)

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_municipio: df_municipio}
    load_data(tables_dfs, client, dataset_fonte)

# Nova função para carregar a dimensão escolaridade_mae
def etl_escolaridade_mae(file_key):
    dataset_name = "mortalidade_infantil"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela escolaridade_mae já existe, se não, cria
    table_escolaridade_mae = table_exist_escolaridade_mae(client, dataset_fonte)

    # Gera o dataframe da dimensão escolaridade_mae
    df_escolaridade_mae = create_df_escolaridade_mae()

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_escolaridade_mae: df_escolaridade_mae}
    load_data(tables_dfs, client, dataset_fonte)

def etl_categoria_cid(file_key):
    dataset_name = "mortalidade_infantil"
    path = "Data/CID-10-CATEGORIAS.csv"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela de município já existe, se não, cria
    table_categoria_cid = table_exist_categoria(client, dataset_fonte)

    # Gera o dataframe da dimensão município
    df_categoria_cid = create_df_categorias_cid(path)

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_categoria_cid: df_categoria_cid}
    load_data(tables_dfs, client, dataset_fonte)

def etl_subcategoria_cid(file_key):
    dataset_name = "mortalidade_infantil"
    path = "Data/CID-10-SUBCATEGORIAS.csv"

    # Conexão com o GCP
    client = gcp_connection(file_key)

    # Verificar se o dataset já existe, se não existe, cria
    dataset_fonte = dataset_exist(client, dataset_name)

    # Verifica se a tabela de município já existe, se não, cria
    table_subcategoria_cid = table_exist_subcategoria(client, dataset_fonte)

    # Gera o dataframe da dimensão município
    df_subcategoria_cid = create_df_subcategorias_cid(path, client, dataset_fonte)

    # Incluir tabela e dataframe em um dicionário para carregamento
    tables_dfs = {table_subcategoria_cid: df_subcategoria_cid}
    load_data(tables_dfs, client, dataset_fonte)

if __name__ == "__main__":
    file_key = "keys/datawarehouse-440722-b55120133f69.json"

    etl_tempo(file_key)
    etl_raca(file_key)
    etl_sexo(file_key)
    etl_municipio(file_key)
    etl_escolaridade_mae(file_key)
    etl_categoria_cid(file_key)
    etl_subcategoria_cid(file_key)
    df_nascidos = etl_nascidos_vivos(file_key)
    etl_mortalidade(file_key, df_nascidos)
