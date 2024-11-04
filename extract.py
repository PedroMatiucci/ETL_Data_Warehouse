# importar bibliotecas
import os
from multiprocessing.resource_sharer import stop
from urllib import request

def download_files_mortalidade(data_folder):
    # Verifica se existe o diretório para os dados, se não existe cria ele
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # baixar os arquivos dos anos desejados
    for ano in range(2010, 2022):
        ano_baixar = str(ano)

        print('-------------------------------------------------')
        print(f'Proximo ano a carregar: {ano_baixar}')
        print('-------------------------------------------------')

        # Definir link e arquivo
        file = f'Mortalidade_Geral_{ano_baixar}.csv'
        link = f'https://diaad.s3.sa-east-1.amazonaws.com/sim/{file}'

        # Verifica se o arquivo foi baixado
        if(os.path.exists(f'{data_folder}/{file}')):
            print(f'Arquivo {file} já foi baixado baixado')
        else:
        # Tenta baixar o arquivo
            try:
                print(f'Baixando o arquivo {file}')
                request.urlretrieve(f'{link}', f'{data_folder}/{file}')
                # Verifica se o arquivo foi baixado
                if(os.path.exists(f'{data_folder}/{file}')):
                    print(f'Arquivo {file} baixado')
                else:
                    print('-------------------------------------------------------')
                    print('Não foi possível baixar o arquivo. Execução finalizada!')
                    print('-------------------------------------------------------')
            except:
                print(f'Arquivos de {ano_baixar} ainda não disponibilizado')
                print('-------------------------------------------------')
                print('Todos os arquivos disponíveis foram baixados!')
                print('-------------------------------------------------')

