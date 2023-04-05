import os 
import pandas as pd
import json
import sqlite3

#Função que recebe path dos arquivos .json e retorna uma lista com os nomes dos arquivos
def encontra_arquivos(path):
    filepaths = []
    extensions = ('.json')
    for root, dirs, files in os.walk(path):
        for filename in files:
            if filename.endswith(extensions):
                if filename == 'arquivoIds.txt':
                    continue
                path_file = os.path.join(root, filename)
                path_file = path_file.split('\\', 4)[4]

                filepaths.append(path_file)

    with open('lista_jsons.txt', 'w', encoding='utf-8') as f:
        for file in filepaths:
            f.write(f'{file} \n')
        
    return filepaths

encontra_arquivos('path')
print('finalizado')

#Função que normaliza os dados dos arquivos .json e retorna um arquivo .txt com as chaves
def encontra_dados():
    #arquivo de entrada
    with open('lista_jsons.txt', 'r', encoding='utf-8') as f:
            path_jsons = f.readlines()
            path_jsons  = [file.replace(' \n', '') for file in path_jsons]
            lista = []
            for file in path_jsons:
                todas_chaves = []
                if 'Arq_Download' not in file:
                    try:
                        with open(file, 'r', encoding='utf-8') as f2:
                            data = json.load(f2)
                        df = pd.json_normalize(data, sep='##')

                    except Exception as e:
                        print(e)
                        print(file)

                with open('dataframe.txt', 'a', encoding='utf-8') as arquivo:
                    for file in df:
                        arquivo.write(f'{file} \n')
#encontra_dados()

def ler_dataframe():
    #arquivo de entrada
    with open('dataframe.txt', 'r', encoding='utf-8') as f3:
        lista = set()
        pagamento = set()
        data = f3.readlines()
        for chave in data:
            chave = chave.strip()
            chave =  chave.split('##', 1)[1]
            chave = chave.replace('##', '.')
            if 'Pagamentos' in chave:
                pagamento.add(chave)
            else:
                lista.add(chave)

    #separando os arquivos para facilitar a criação das tabelas
    with open('dataframe.txt', 'w', encoding='utf-8') as f3:
        for chave in lista:
            f3.write(chave + '\n')
    with open('dataframe.txt', 'w', encoding='utf-8') as f2:
        for chave in pagamento:
            f2.write(chave + '\n')
#ler_dataframe()

#Função para ordenar as chaves
def ordena():
    lista = []
    #arquivo de entrada
    with open('arquivos.txt', 'r', encoding='utf-8') as f:
        data = f.readlines()
        for dados in data:
            dados = dados.strip()
            # dados = dados.split('.', 3)[3]
            lista.append(dados)
        lista.sort()

    #arquivo de saída
    with open('arquivos.txt', 'w', encoding='utf-8') as f2:
        for dados in lista:
            f2.write(dados + '\n')
ordena()

# cria uma conexão com o banco de dados
conn = sqlite3.connect('banco.db')
cursor = conn.cursor()


def drop_tables():
    with open('arquivos.txt') as f:
        dados = f.readlines()
        for i in dados:
            i = i.strip()
            table, key = i.split('.')
            table = table.lower()
            table = table.replace(' ', '_') if ' ' in table else table
            key = key.lower()
            key = key.replace('/', '_') if '/' in key else key
            key = key.replace('(', '_') if '(' in key else key
            key = key.replace(')', '_') if ')' in key else key

            # Apaga as tabelas            
            cursor.execute(f'DROP TABLE IF EXISTS {table}')
            print(f'tabela {table} apagada')

def create_tables():
    # abre o arquivo JSON e carrega os dados em um DataFrame
    with open('arquivos.txt') as f:
        dados = f.readlines()
        tables = []
        for i in dados:
            i = i.strip()
            table, key = i.split('.')
            table = table.lower()
            table = table.replace(' ', '_') if ' ' in table else table
            key = key.lower()
            key = key.replace('/', '_') if '/' in key else key
            key = key.replace('(', '_') if '(' in key else key
            key = key.replace(')', '_') if ')' in key else key
            key = key.replace(' ', '_') if ' ' in key else key

            # Cria as tabelas
            if table not in tables:
                if '*' in key and not ('data' in key or 'numero' in key):
                    key = key.replace('*', '')

                    cursor.execute(f'CREATE TABLE IF NOT EXISTS {table} \
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
                                    n_processo INTEGER, {key} TEXT NOT NULL)')
                    conn.commit()
                    print(
                        f'tabela {table} criada e coluna {key} adicionada. Tipo Texto NOT NULL')

                elif 'data' in key:
                    key = key.replace('*', '') if '*' in key else key
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} \
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
                                    n_processo INTEGER, {key} DATE)")
                    conn.commit()
                    print(
                        f'tabela {table} criada e coluna {key} adicionada. Tipo DATE')

                elif 'numero' in key:
                    key = key.replace('*', '') if '*' in key else key
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} \
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
                                    n_processo INTEGER, {key} INTEGER)")
                    conn.commit()
                    print(
                        f'tabela {table} criada e coluna {key} adicionada. Tipo INTEGER')
                    
                elif 'valor' in key:
                    key = key.replace('*', '') if '*' in key else key
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} \
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
                                    n_processo INTEGER, {key} FLOAT)")
                    conn.commit()
                    print(
                        f'tabela {table} criada e coluna {key} adicionada. Tipo FLOAT')

                else:
                    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} \
                                    (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
                                    n_processo INTEGER, {key} TEXT)")
                    conn.commit()
                    print(
                        f'tabela {table} criada e coluna {key} adicionada. Tipo TEXT')

                tables.append(table)

            # Adiciona as colunas
            else:
                if '*' in key and not ('data' in key or 'numero' in key):
                    key = key.replace('*', '')

                    cursor.execute(
                        f"ALTER TABLE {table} ADD COLUMN {key} TEXT NOT NULL")
                    conn.commit()
                    print(f'coluna {key} adicionada. Tipo Texto NOT NULL')

                elif 'data' in key:
                    key = key.replace('*', '') if '*' in key else key
                    cursor.execute(
                        f"ALTER TABLE {table} ADD COLUMN {key} DATE")
                    conn.commit()
                    print(f'coluna {key} adicionada. Tipo Data')
                
                elif 'valor' in key:
                    key = key.replace('*', '') if '*' in key else key
                    cursor.execute(
                        f"ALTER TABLE {table} ADD COLUMN {key} FLOAT")
                    conn.commit()
                    print(f'coluna {key} adicionada. Tipo float')

                elif 'numero' in key:
                    key = key.replace('*', '') if '*' in key else key
                    cursor.execute(
                        f"ALTER TABLE {table} ADD COLUMN {key} INTEGER")
                    conn.commit()
                    print(f'coluna {key} adicionada. Tipo Integer')

                else:
                    cursor.execute(
                        f"ALTER TABLE {table} ADD COLUMN {key} TEXT")
                    conn.commit()
                    print(f'coluna {key} adicionada. Tipo Texto')

    # fecha a conexão
    conn.close()

create_tables()
#drop_tables()
