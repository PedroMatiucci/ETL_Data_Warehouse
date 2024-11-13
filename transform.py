# importar bibliotecas
import os
import pandas as pd


def create_df_natalidade(data_folder):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)

    print("--------------------------------------------------------------------------")
    print("Carregando os dados dos arquivos extraídos, tratando e concatenando...")

    # lista com os dataframes já tratados
    dfs = []

    # Função para gerar o dataframe
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # Ler o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', encoding='ISO-8859-1', low_memory=False)
            df = df[['CODMUNRES', 'DTNASC']]

            # Realizar transformação das datas de nascimento
            df['dt_nasc'] = pd.to_datetime(df['DTNASC'], format='%d%m%Y', errors='coerce')
            # Excluir dados nulos para data de nascimento
            df = df.dropna(subset=['dt_nasc'])

            # Criar as colunas ano_nasc e quadrimestre_nasc e mes_nasc
            df['ano'] = df['dt_nasc'].dt.year.astype(float).astype(pd.Int64Dtype()).astype(str).where(
                df['dt_nasc'].notna())
            df['mes'] = df['dt_nasc'].dt.month
            # Extrair os 6 primeiros dígitos da coluna CODMUNRES
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)

            # Selecionar coluna desejadas
            df = df[['ano', 'dt_nasc', 'mes', 'cd_mun_res']]
            # adiciona o dataframe à lista de dataframes
            dfs.append(df)
            print('arquivo concluido indo para o prox')

    # concatena os dataframes em um único dataframe final
    df_group = pd.concat(dfs, ignore_index=True)
    grupos = df_group.groupby(['cd_mun_res', 'mes', 'ano'])
    df_group['total_nascidos'] = grupos['dt_nasc'].transform('count')
    df_agrupado_natalidade = df_group[['cd_mun_res', 'ano', 'mes']].groupby(
        ['cd_mun_res', 'ano', 'mes']).sum().reset_index()
    df_natalidade = df_agrupado_natalidade.merge(
        df_group[['cd_mun_res', 'ano', 'mes', 'total_nascidos']].drop_duplicates(), on=['cd_mun_res', 'ano', 'mes'],
        how='left')
    df_natalidade = df_natalidade.dropna(subset=['total_nascidos'])

    return df_natalidade

def create_df_mortalidade(data_folder, client, dataset_fonte):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)

    print("--------------------------------------------------------------------------")
    print("Carregando os dados dos arquivos extraídos, tratando e concatenando...")

    # Carregar a tabela dim_tempo
    table_tempo = client.get_table(dataset_fonte.table("dim_tempo"))
    df_tempo = client.list_rows(table_tempo).to_dataframe()

    # Carregar a tabela dim_municipio
    table_municipio = client.get_table(dataset_fonte.table("dim_municipio"))
    df_municipio = client.list_rows(table_municipio).to_dataframe()

    # Converter a coluna 'ibge' para string em df_municipio
    df_municipio['ibge'] = df_municipio['ibge'].astype(str)

    # Lista com os dataframes já tratados
    dfs = []

    # Função para gerar o dataframe
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # Ler o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', encoding='ISO-8859-1', low_memory=False)
            #Seleciona Apenas as Colunas que vamos Utilizar
            df = df[['DTOBITO', 'DTNASC', 'CODMUNRES', 'SEXO', 'RACACOR', 'ESCMAE', 'CAUSABAS']]

            # Realizar transformação das datas de nascimento e óbito
            df['dt_obito'] = pd.to_datetime(df['DTOBITO'], format='%d%m%Y', errors='coerce')
            df['dt_nasc'] = pd.to_datetime(df['DTNASC'], format='%d%m%Y', errors='coerce')

            # Excluir dados nulos para data de nascimento e de óbito
            df = df.dropna(subset=['dt_nasc', 'dt_obito'])

            # Criar a coluna idade em dias e filtrar por idades válidas e menores de 28 dias
            df['idade'] = (df['dt_obito'] - df['dt_nasc']).dt.days
            df = df[(df['idade'] >= 0) & (df['idade'] <= 28)]

            # Criar as colunas ano_obito e mes_obito
            df['ano_obito'] = df['dt_obito'].dt.year
            df['mes_obito'] = df['dt_obito'].dt.month

            # Fazer join com dim_tempo para substituir ano e mes pelo id de tempo
            df = df.merge(df_tempo, left_on=['ano_obito', 'mes_obito'], right_on=['ano', 'mes'], how='left')
            df = df.rename(columns={'id': 'dim_tempo_id'})

            # Extrair os 6 primeiros dígitos da coluna CODMUNRES, converter para string e fazer o join com dim_municipio
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)

            # Fazer join com dim_municipio para substituir cd_mun_res pelo id do município
            df = df.merge(df_municipio[['ibge', 'id']], left_on='cd_mun_res', right_on='ibge', how='left')
            df = df.rename(columns={'id': 'dim_municipio_id'})

            # Selecionar colunas desejadas e remover ano_obito, mes_obito, cd_mun_res e ibge
            df = df[['dim_tempo_id', 'dim_municipio_id', 'dt_obito', 'dt_nasc', 'SEXO', 'RACACOR', 'ESCMAE', 'CAUSABAS']]
            dfs.append(df)

    # Concatenar os dataframes em um único dataframe final
    df_final = pd.concat(dfs, ignore_index=True)

    # Calcular numero_obitos antes do agrupamento final
    df_final['numero_obitos'] = df_final.groupby(
        ['dim_tempo_id', 'dim_municipio_id', 'SEXO', 'RACACOR', 'ESCMAE', 'CAUSABAS']
    )['dim_tempo_id'].transform('count')

    # Selecionar colunas que não são datetime para agrupar com sum
    cols_to_sum = df_final.columns.difference(['dt_obito', 'dt_nasc'])

    # Agrupar os dados por dim_tempo_id e outras colunas especificadas, mantendo numero_obitos
    df_grouped = df_final[cols_to_sum].drop_duplicates().groupby(
        ['dim_tempo_id', 'dim_municipio_id', 'SEXO', 'RACACOR', 'ESCMAE', 'CAUSABAS']
    ).sum().reset_index()

    return df_grouped


# Nova função para criar a dimensão tempo
def create_df_tempo(start_year=2010, end_year=2020):
    """
    Gera a dimensão tempo com colunas 'ano', 'mes' e um ID único para cada combinação.
    """
    # Gera uma lista de todos os meses e anos no intervalo desejado
    anos = range(start_year, end_year + 1)
    meses = range(1, 13)
    dim_tempo = pd.DataFrame([(ano, mes) for ano in anos for mes in meses], columns=['ano', 'mes'])

    # Gera uma coluna ID única para cada combinação de ano e mês
    dim_tempo['id'] = range(1, len(dim_tempo) + 1)

    return dim_tempo

# Nova função para criar a dimensão raca
def create_df_raca():
    """
    Gera a dimensão raca com colunas 'id' e 'raca' para as categorias de raça.
    """
    data = {
        'id': [1, 2, 3, 4, 5],
        'raca': ['Branca', 'Preta', 'Amarela', 'Parda', 'Indígena']
    }
    dim_raca = pd.DataFrame(data)
    return dim_raca

# Nova função para criar a dimensão sexo
def create_df_sexo():
    """
    Gera a dimensão sexo com colunas 'id' e 'sexo' para as categorias de sexo.
    """
    data = {
        'id': [1, 2, 9],
        'sexo': ['Masculino', 'Feminino', 'Ignorado']
    }
    dim_sexo = pd.DataFrame(data)
    return dim_sexo

# Nova função para criar a dimensão municipio
def create_df_municipio(data_folder):
    """
    Gera a dimensão municipio a partir de um arquivo CSV com colunas 'IBGE', 'UF' e 'Municipio',
    ignorando a coluna 'Tipologia IBGE' se presente.
    """
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)

    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            try:
                # Carrega o arquivo CSV em um dataframe com codificação utf-8
                df_municipio = pd.read_csv(
                    os.path.join(file_path, arquivo), delimiter=',', encoding='utf-8', low_memory=False
                )
            except UnicodeDecodeError:
                # Se utf-8 falhar, tenta com ISO-8859-1
                df_municipio = pd.read_csv(
                    os.path.join(file_path, arquivo), delimiter=',', encoding='ISO-8859-1', low_memory=False
                )

            # Renomeia colunas para seguir a convenção do DW
            df_municipio = df_municipio.rename(columns={'IBGE': 'ibge', 'UF': 'uf', 'Municipio': 'municipio'})

            # Remove a coluna 'Tipologia IBGE' se estiver presente
            if 'Tipologia IBGE' in df_municipio.columns:
                df_municipio = df_municipio.drop(columns=['Tipologia IBGE'])

            # Cria uma coluna ID único para a tabela dimensão
            df_municipio['id'] = range(1, len(df_municipio) + 1)

    return df_municipio

# Função para criar a dimensão escolaridade_mae
def create_df_escolaridade_mae():
    """
    Gera a dimensão escolaridade_mae com colunas 'id' e 'descricao' para as categorias de escolaridade.
    """
    data = {
        'id': [0, 1, 2, 3, 4, 5, 9],
        'descricao': [
            'Sem escolaridade',
            'Fundamental I (1ª a 4ª série)',
            'Fundamental II (5ª a 8ª série)',
            'Médio (antigo 2º Grau)',
            'Superior incompleto',
            'Superior completo',
            'Ignorado'
        ]
    }
    dim_escolaridade_mae = pd.DataFrame(data)
    return dim_escolaridade_mae