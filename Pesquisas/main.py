from functions.api_ipesquisa import IPesquisaAPI as ip
from Agora import pesquisas as p
from queries_and_variables import variables as vs, variables_pipefy as vp, queries_pipefy as qp
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



for index, row in questionario.iterrows():
    df = api_ipesquisa.get_csv_cases_by_id(row[0], dt_gravacao_inicio=row[3], dt_gravacao_fim=row[4])
    
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


if log_messages:
    log.warning("\n".join(log_messages))

Mysql.close()