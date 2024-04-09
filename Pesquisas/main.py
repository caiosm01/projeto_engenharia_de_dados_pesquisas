from functions.api_ipesquisa import IPesquisaAPI as ip
from SAC import satisfacao as s, medicao as m, satisfacao_ingles as si, escala as e, meta as mt
from Agora import pesquisas as p
from queries_and_variables import variables_sac as vs, variables_pipefy as vp, queries_pipefy as qp
from functions import Database as DB, api_pipefy as AP
import logging
from dotenv import load_dotenv
from Agora import projetos as PJ, funcionarios as FC
import sys
import os
import pandas as pd


diretorio_superior = os.path.abspath('..')
if diretorio_superior not in sys.path:
    sys.path.append(diretorio_superior)

################################################### VARIÁVEIS PRIVADAS ###############################################
load_dotenv(override=True)
usuario_maquina = os.getenv("usuario_maquina")
host = os.getenv("host")
database = os.getenv("database")
user = os.getenv("user")
password = os.getenv("password")
registros = os.getenv("registros")
usuario_ipesquisa = os.getenv("usuario_ipesquisa")
senha_ipesquisa = os.getenv("senha_ipesquisa")
Escala_meta = os.getenv("Escala_meta")

################################################### CONFIGURAÇÃO DO LOG ##############################################
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(filename=f"C:/Users/{usuario_maquina}/OneDrive - agorap.com.br/MySQL/DW_Agora.log", level=logging.WARNING, format=LOG_FORMAT)
log = logging.getLogger()
log_messages = []

########################################### INSTÂNCIA DO BANCO DE DADOS ##############################################
#Mysql = DB.DatabaseConnection(host='20.168.42.226', database='banco_sac', user='Geral', password='#Agora123#')
Mysql = DB.DatabaseConnection(host=host, database=database, user=user, password=password)
Mysql.connect()

########################################### INSTÂNCIA DA TRATAMENTO DOS PROJETOS #####################################
projeto_processor = PJ.DataProjetosProcessor(Mysql, vp.caminho_json_projetos, vp.field_mapping_projetos)

########################################### INSTÂNCIA DA TRATAMENTO DOS FUNCIONÁRIOS #################################
funcionarios_processor = FC.DataFuncionariosProcessor(Mysql, vp.caminho_json_funcionarios)


########################################### INSTÂNCIA DA API DO PIPEFY ############################################
api_manager = AP.ApiManager(Mysql)

########################################### INSTÂNCIA DA API DO IPESQUISA ############################################
api_ipesquisa = ip(usuario_ipesquisa, senha_ipesquisa)


questionario = Mysql.fetch_all("SELECT * FROM mydb.i_pesquisa where status = 1;")

#print(questionario)


for index, row in questionario.iterrows():
    df = api_ipesquisa.get_csv_cases_by_id(row[0], dt_gravacao_inicio=row[3], dt_gravacao_fim=row[4])
    #print(df)
    result_pesquisa = p.pesquisas(Mysql, df, vs, row[1], row[0])

    if result_pesquisa[0] == 1:
        api = api_manager.chamar_api(qp.query_pipe_projetos, vp.caminho_json_projetos)
        projct = projeto_processor.update_database('myDB.projetos', 'idProjeto')
        log_messages.append(api)
        log_messages.append(projct)

        result_pesquisa = p.pesquisas(Mysql, df, vs, row[1], row[0])

    elif result_pesquisa[0] == 2:
        api = api_manager.chamar_api(qp.query_database_colaboradores, vp.caminho_json_funcionarios)
        func = funcionarios_processor.update_database('myDB.funcionarios', 'idFuncionario')
        log_messages.append(api)
        log_messages.append(func)
        result_pesquisa = p.pesquisas(Mysql, df, vs, row[1], row[0])

    log_message = f"{row[2]}: {result_pesquisa[1]}"
    print(log_message)
    log_messages.append(log_message)

    func_map = [s.satisfacao,m.medicao, si.satisfacao_ingles]

    for f in func_map:
        try:
            result_tipo = f(Mysql, df, vs, row[1])
            log_message = f"{row[2]}: {result_tipo[1]}"
            print(log_message)
            log_messages.append(log_message)

        except KeyError:
            print(f"{row[2]}: Não é uma pesquisa da SAC.")



df_escala = pd.read_excel(Escala_meta, sheet_name='Base_Escala', header=4)
result_escala = e.Escala(Mysql, df_escala, vs)
log_message = f"Escala: {result_escala}"
log_messages.append(log_message)

df_meta = pd.read_excel(Escala_meta, sheet_name='Base_Meta')
result_meta = mt.Meta(Mysql, df_meta, vs)
log_message = f"Meta: {result_meta}"
log_messages.append(log_message)

if log_messages:
    log.warning("\n".join(log_messages))

Mysql.close()