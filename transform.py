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

            # Criar as colunas ano e mes como strings
            df['ano'] = df['dt_nasc'].dt.year.astype(str)
            df['mes'] = df['dt_nasc'].dt.month.astype(str)
            # Extrair os 6 primeiros dígitos da coluna CODMUNRES
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)

            # Selecionar coluna desejadas
            df = df[['ano', 'mes', 'cd_mun_res']]
            # adiciona o dataframe à lista de dataframes
            dfs.append(df)
            print('Arquivo concluído, indo para o próximo')

    # Concatenar os dataframes em um único dataframe final
    df_group = pd.concat(dfs, ignore_index=True)
    grupos = df_group.groupby(['cd_mun_res', 'mes', 'ano'])
    df_group['total_nascidos'] = grupos['mes'].transform('count')
    df_agrupado_natalidade = df_group[['cd_mun_res', 'ano', 'mes']].groupby(
        ['cd_mun_res', 'ano', 'mes']).sum().reset_index()
    df_natalidade = df_agrupado_natalidade.merge(
        df_group[['cd_mun_res', 'ano', 'mes', 'total_nascidos']].drop_duplicates(), on=['cd_mun_res', 'ano', 'mes'],
        how='left')
    df_natalidade = df_natalidade.dropna(subset=['total_nascidos'])

    return df_natalidade

def create_df_mortalidade(data_folder, client, dataset_fonte, df_natalidade):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)

    print("--------------------------------------------------------------------------")
    print("Carregando os dados dos arquivos extraídos, tratando e concatenando...")

    # Carregar a tabela dim_tempo
    table_tempo = client.get_table(dataset_fonte.table("dim_tempo"))
    df_tempo = client.list_rows(table_tempo).to_dataframe()


    table_cid_categoria = client.get_table(dataset_fonte.table("dim_categoriacid"))
    df_cid_categoria = client.list_rows(table_cid_categoria).to_dataframe()

    table_cid_subcategoria = client.get_table(dataset_fonte.table("dim_subcategoriacid"))
    df_cid_subcategoria = client.list_rows(table_cid_subcategoria).to_dataframe()


    # Converter colunas 'ano' e 'mes' em df_tempo para string
    df_tempo['ano'] = df_tempo['ano'].astype(str)
    df_tempo['mes'] = df_tempo['mes'].astype(str)

    # Carregar a tabela dim_municipio
    table_municipio = client.get_table(dataset_fonte.table("dim_municipio"))
    df_municipio = client.list_rows(table_municipio).to_dataframe()

    # Converter as colunas 'ibge' para string em df_municipio
    df_municipio['ibge'] = df_municipio['ibge'].astype(str)

    # Garantir que a coluna cd_mun_res em df_natalidade seja string
    df_natalidade['cd_mun_res'] = df_natalidade['cd_mun_res'].astype(str)

    # Lista com os dataframes já tratados
    dfs = []

    # Processar arquivos de mortalidade
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # Ler o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', encoding='ISO-8859-1', low_memory=False)
            # Seleciona Apenas as Colunas que vamos Utilizar
            df = df[['DTOBITO', 'CODMUNRES', 'SEXO', 'RACACOR', 'ESCMAE', 'CAUSABAS']]

            # Realizar transformação das datas de óbito
            df['dt_obito'] = pd.to_datetime(df['DTOBITO'], format='%d%m%Y', errors='coerce')

            # Excluir dados nulos para data de óbito
            df = df.dropna(subset=['dt_obito'])

            # Criar as colunas ano e mes e converter para string
            df['ano'] = df['dt_obito'].dt.year.astype(str)
            df['mes'] = df['dt_obito'].dt.month.astype(str)

            # Garantir que 'cd_mun_res' seja string antes do merge
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)

            # Adicionar ao dataframe
            dfs.append(df)

    # Concatenar os dataframes em um único dataframe final
    df_final = pd.concat(dfs, ignore_index=True)

    # Mapear `cd_mun_res` para o ID do município
    df_final = df_final.merge(df_municipio[['ibge', 'id']], left_on='cd_mun_res', right_on='ibge', how='left')
    df_final = df_final.rename(columns={'id': 'dim_municipio_id'})

    # Calcular número de óbitos considerando as características de morte
    df_final['numero_obitos'] = df_final.groupby(
        ['ano', 'mes', 'dim_municipio_id', 'SEXO', 'RACACOR', 'ESCMAE', 'CAUSABAS']
    )['dim_municipio_id'].transform('count')

    # Mesclar com dados de natalidade para adicionar `total_nascidos`
    df_final = df_final.merge(
        df_natalidade[['ano', 'mes', 'cd_mun_res', 'total_nascidos']],
        left_on=['ano', 'mes', 'cd_mun_res'],
        right_on=['ano', 'mes', 'cd_mun_res'],
        how='left'
    )

    # Calcular a taxa de mortalidade
    df_final['taxa_mortalidade'] = (df_final['numero_obitos'] / df_final['total_nascidos']) * 1000

    # Substituir ano e mes pelos IDs da dimensão tempo
    df_final = df_final.merge(df_tempo, on=['ano', 'mes'], how='left')
    df_final = df_final.rename(columns={'id': 'dim_tempo_id'})

    df_final['codigo_categoria'] = df_final['CAUSABAS'].str[:3]
    df_final = df_final.merge(df_cid_categoria[['codigo_categoria', 'id']], on='codigo_categoria', how='left')
    df_final = df_final.rename(columns={'id': 'dim_categoria_cid_id'})

    df_final = df_final.merge(df_cid_subcategoria[['codigo_subcategoria', 'id']], left_on='CAUSABAS', right_on=['codigo_subcategoria'], how='left')
    df_final = df_final.rename(columns={'id': 'dim_subcategoria_cid_id'})

    # Renomear colunas para apontar para tabelas dimensão
    df_final = df_final.rename(columns={
        'SEXO': 'dim_sexo_id',
        'RACACOR': 'dim_raca_id',
        'ESCMAE': 'dim_escolaridade_mae_id'
    })

    # Selecionar colunas finais
    df_final = df_final[['dim_tempo_id', 'dim_municipio_id', 'dim_sexo_id', 'dim_raca_id',
                         'dim_escolaridade_mae_id', 'CAUSABAS', 'numero_obitos', 'total_nascidos', 'taxa_mortalidade', 'dim_categoria_cid_id', 'dim_subcategoria_cid_id']]

    return df_final

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



def create_df_categorias_cid(file_path):
    try:
        # Carrega o arquivo CSV em um dataframe com codificação utf-8
        df_cid = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1', low_memory=False)

    except UnicodeDecodeError:
        # Se utf-8 falhar, tenta com ISO-8859-1
        df_cid = pd.read_csv(file_path, delimiter=';', encoding='utf-8', low_memory=False
                             )

    # Renomeia colunas para seguir a convenção do DW
    df_cid = df_cid.drop(columns=['CLASSIF', 'DESCRABREV', 'REFER', 'EXCLUIDOS'])
    df_cid = df_cid.rename(columns={'CAT': 'codigo_categoria', 'DESCRICAO': 'descricao'})

    # Cria uma coluna ID único para a tabela dimensão
    df_cid['id'] = range(1, len(df_cid) + 1)

    df_cid = df_cid[['codigo_categoria', 'id', 'descricao']]

    return df_cid

def create_df_subcategorias_cid(file_path):
    try:
        # Carrega o arquivo CSV em um dataframe com codificação utf-8
        df_cid = pd.read_csv(file_path, delimiter=';', encoding='ISO-8859-1', low_memory=False)

    except UnicodeDecodeError:
        # Se utf-8 falhar, tenta com ISO-8859-1
        df_cid = pd.read_csv(file_path, delimiter=';', encoding='utf-8', low_memory=False
                             )
    df_cid = df_cid.drop(columns=['CLASSIF', 'DESCRABREV', 'REFER', 'EXCLUIDOS', 'RESTRSEXO', 'CAUSAOBITO', 'DESCRABREV', 'REFER', 'EXCLUIDOS'])
    df_cid = df_cid.rename(columns={'SUBCAT': 'codigo_subcategoria', 'DESCRICAO': 'descricao'})

    # Cria uma coluna ID único para a tabela dimensão
    df_cid['id'] = range(1, len(df_cid) + 1)
    df_cid = df_cid[['codigo_subcategoria', 'id', 'descricao']]

    return df_cid