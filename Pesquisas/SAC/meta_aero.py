import mysql.connector
import pandas as pd
import pymysql
from mysql.connector import Error
import numpy as np
from datetime import date
from datetime import datetime


con = mysql.connector.connect(host='127.0.0.1', database='banco_sac', user='root', password='#Agora123#')

Consulta = r"H:\Meu Drive\Projetos\Ativos\SAC\03 - Março 24\SAC - 07 - Escala  e Metas Completa - Março 24.xlsx"

db_info = con.get_server_info()
#print(db_info)
cursor = con.cursor()
cursor.execute("select * from aeroportos;")
aeroportos = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

cursor.execute("select * from estrato;")
estrato = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

cursor.execute("select * from processo;")
processo = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

cursor.execute("select * from tipo_procedimento;")
tipo_procedimento = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

cursor.execute("select * from companhia_aerea;")
companhia_aerea = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)


cursor.execute("select * from meta_pesquisadores;")
metas = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)

cursor.execute("select * from escala_pesquisadores;")
escala = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)


################################################TRATAMENTO DA BASE DE METAS ####################################################


df_meta = pd.read_excel(Consulta, sheet_name='PA - Medição - Original')
#pesquisadores = pd.read_excel(Consulta, sheet_name='Base_AUX', header=0)
df_meta.columns = df_meta.columns.str.strip()
df_meta = df_meta[['AEROPORTO', 'PROCESSOS', 'ESTRATO', 'CIA', 'PROCEDIMENTO', 'AMOSTRA']]
df_meta = df_meta.astype(str)

for coluna in df_meta.columns:
    if df_meta[coluna].dtype == 'object':
        df_meta[coluna] = df_meta[coluna].str.strip()
        df_meta[coluna] = df_meta[coluna].str.normalize('NFC')
    else:
        continue

df_meta["CIA"] = df_meta["CIA"].replace('nan', 'N/A')
#df_meta = df_meta.replace('nan', '')
df_meta['PROCESSOS'] = df_meta['PROCESSOS'].replace('Check-In', 'Check In')
df_meta['PROCESSOS'] = df_meta['PROCESSOS'].str.capitalize()

df_meta = df_meta.rename( columns={'AEROPORTO': 'ICAO', 'PROCESSOS': 'Processo', 'ESTRATO': 'Estrato', 'CIA': 'Companhia'})

df_meta = pd.merge(df_meta, aeroportos.drop(columns=['Aeroporto', 'Categoria', 'Nome_Aeroporto', 'Latitude', 'Longitude']).astype(str), how='left', on='ICAO')
df_meta = pd.merge(df_meta, processo.drop(columns=['parametro_tempo_domestico', 'parametro_tempo_int']).astype(str),
                   how='left', on='Processo')
df_meta = pd.merge(df_meta, estrato, how='left', on='Estrato')
#df_meta = pd.merge(df_meta, pesquisadores[['Pesquisador', 'ID PIPE']], how='left', on='Pesquisador')
df_meta = pd.merge(df_meta, companhia_aerea, how='left', on='Companhia')
df_meta = df_meta.drop(['ICAO', 'Estrato', 'Processo', 'Companhia'], axis=1)
df_meta['PROCEDIMENTO'] = df_meta['PROCEDIMENTO'].replace('Medição', '2')
df_meta['PROCEDIMENTO'] = df_meta['PROCEDIMENTO'].replace('Pesquisa', '1')

df_meta = df_meta.rename(columns={'PROCEDIMENTO': 'id_tipo', 'ID PIPE': 'Pesquisador'})

df_meta['AMOSTRA'] = pd.to_numeric(df_meta['AMOSTRA'].replace('', 0), downcast="integer")

df_meta['AMOSTRA'] = df_meta['AMOSTRA'].round(0)

df_meta = df_meta[['id_aeroportos', 'id_processo', 'id_estrato', 'id_companhia', 'id_tipo', 'AMOSTRA']]
#df_meta = df_meta[pd.isna(df_meta['Pesquisador']) == False]
df_meta = df_meta[pd.isna(df_meta['id_aeroportos']) == False]

df_meta['Mês'] = str(date.today().month)
# df_meta['Mês'] = str(12)
df_meta['Ano'] = str(date.today().year)
# df_meta['Ano'] = str(2023)

'''df_meta['id'] = df_meta['Pesquisador'].astype(str) + ' / ' + df_meta['id_aeroportos'].astype(str) + ' / ' + df_meta[
    'id_processo'].astype(str) + ' / ' + df_meta['id_estrato'].astype(str) + ' / ' + df_meta['id_companhia'].astype(
    str) + ' / ' + df_meta['id_tipo'].astype(str) + ' / ' + df_meta['Mês'].astype(str) + ' / ' + df_meta['Ano'].astype(
    str)'''

print(df_meta)


for index, row in df_meta.iterrows():
    try:
        row = row.where(pd.notnull(row), None)
        caractere = ("%s, "*int(len(df_meta.columns)-1))+"%s"

        # Preparar a consulta SQL com parâmetros
        sql_query = f"""INSERT INTO banco_sac2.meta_por_aero (
                    id_aeroportos,
                    id_processo,
                    id_estrato,
                    id_companhia,
                    id_tipo,
                    META,
                    Mês,
                    Ano)
                    VALUES ({caractere});"""

         # Executar a consulta com os valores da linha
        cursor.execute(sql_query, tuple(row))


        con.commit()
        #print(tuple(row))
    except Error as e:
    # Ignora erros específicos ou imprime outros
        if e.errno == 1452:
            print(e)
            pass
        elif e.errno == 1062:
            print(e)
            pass
        else:
            # Para outros erros, levantar a exceção
            raise


cursor.close()
con.close()
