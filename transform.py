# importar bibliotecas
import os
import pandas as pd


def create_df_natalide(data_folder):
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


def create_df_mortalidade(data_folder):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, data_folder)
    population_path = r'Data/populacao.csv'

    print("--------------------------------------------------------------------------")
    print("Carregando os dados dos arquivos extraídos, tratando e concatenando...")

    # lista com os dataframes já tratados
    dfs = []

    # Função para gerar o dataframe
    for arquivo in os.listdir(file_path):
        if arquivo.endswith('.csv'):
            # Ler o arquivo CSV com o pandas
            df = pd.read_csv(os.path.join(file_path, arquivo), delimiter=';', encoding='ISO-8859-1', low_memory=False)
            # Realizar transformação das datas de nascimento e óbito
            df['dt_obito'] = pd.to_datetime(df['DTOBITO'], format='%d%m%Y', errors='coerce')
            df['dt_nasc'] = pd.to_datetime(df['DTNASC'], format='%d%m%Y', errors='coerce')
            # Excluir dados nulos para data de nascimento e de óbito
            df = df.dropna(subset=['dt_nasc'])
            df = df.dropna(subset=['dt_obito'])
            # Criar a coluna idade em dias
            df['idade'] = ((df['dt_obito'] - df['dt_nasc']).dt.days)
            # Manter apenas dados com idades válidas
            df = df[df['idade'] >= 0]
            # Manter apenas dados de Menores de 5 anos
            df = df[df['idade'] <= 28]
            # Criar as colunas ano_obito e quadrimestre_obito
            df['ano_obito'] = df['dt_obito'].dt.year.astype(float).astype(pd.Int64Dtype()).astype(str).where(
                df['dt_obito'].notna())
            df['quad_obito'] = pd.cut(df['dt_obito'].dt.month, bins=[1, 5, 9, 13], labels=[1, 2, 3], right=False)
            # Extrair os 6 primeiros dígitos da coluna CODMUNRES
            df['cd_mun_res'] = df['CODMUNRES'].astype(str).str.slice(stop=6)
            # Selecionar coluna desejadas
            df = df[['ano_obito', 'quad_obito', 'dt_obito', 'dt_nasc', 'idade', 'cd_mun_res']]
            # adiciona o dataframe à lista de dataframes
            dfs.append(df)

    # concatena os dataframes em um único dataframe final
    df_group = pd.concat(dfs, ignore_index=True)
    # Baixa a base de população por município
    df_populacao = pd.read_csv(population_path, parse_dates=[0])
    # Transformar id_municipio para apenas 6 digitos
    df_populacao['id_municipio'] = df_populacao['id_municipio'].astype(str).str[:6]
    # Alterar tipo de dados do ano
    df_populacao['data'] = pd.to_datetime(df_populacao.ano, format='%Y', errors='coerce')
    # Adicionar Coluna Para os quadrimestres
    df_populacao['quad'] = pd.cut(df_populacao['data'].dt.month, bins=[1, 5, 9, 13], labels=[1, 2, 3], right=False)
    df_populacao['ano'] = df_populacao['data'].dt.strftime('%Y')
    df_populacao['quad'] = df_populacao['quad'].astype('int')
    df_populacao.rename(columns={"id_municipio": "cd_mun_res"}, inplace=True)
    df_group.rename(columns={"ano_obito": "ano", "quad_obito": "quad"}, inplace=True)
    # Merge os dataframes com base nas condições especificadas
    df_mortalidade_infantil = df_group.merge(df_populacao[['cd_mun_res', 'ano', 'quad', 'populacao']],
                                             on=['ano', 'quad', 'cd_mun_res'], how='left')
    df_mortalidade_infantil.drop(['ano'], axis=1, inplace=True)
    # df_mortalidade_infantil['ano_obito'] = df_mortalidade_infantil['dt_obito'].dt.year.astype(float).astype(pd.Int64Dtype()).astype(str).where(df_mortalidade_infantil['dt_obito'].notna())
    df_mortalidade_infantil.info()

    return df_mortalidade_infantil
