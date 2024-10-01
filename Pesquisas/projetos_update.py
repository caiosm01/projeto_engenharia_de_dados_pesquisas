from queries_and_variables import variables as vs, variables_pipefy as vp
from functions import Database as DB, api_pipefy as AP, JasonDataframe as JD,Dataframe as DF
from queries_and_variables import queries_pipefy as qp
from dotenv import load_dotenv
from Agora import projetos as PJ, funcionarios as FC
from functions.api_ipesquisa import IPesquisaAPI as ip
import sys
import os
from datetime import date,timedelta


diretorio_superior = os.path.abspath('..')
if diretorio_superior not in sys.path:
    sys.path.append(diretorio_superior)

################################################### VARIÁVEIS PRIVADAS ###############################################
load_dotenv(override=True)

host = os.getenv("host")
database = os.getenv("database")
user = os.getenv("user")
password = os.getenv("password")
usuario_ipesquisa = os.getenv("usuario_ipesquisa")
senha_ipesquisa = os.getenv("senha_ipesquisa")


########################################### INSTÂNCIA DO BANCO DE DADOS ##############################################
Mysql = DB.DatabaseConnection(host=host, database=database, user=user, password=password)
Mysql.connect()

########################################### INSTÂNCIA DA TRATAMENTO DOS PROJETOS #####################################
projeto_processor = PJ.DataProjetosProcessor(Mysql, vp.caminho_json_projetos, vp.field_mapping_projetos)

########################################### INSTÂNCIA DA TRATAMENTO DOS FUNCIONÁRIOS #################################
funcionarios_processor = FC.DataFuncionariosProcessor(Mysql, vp.caminho_json_funcionarios)

########################################### INSTÂNCIA DA API DO IPESQUISA ############################################
api_manager = AP.ApiManager(Mysql)
api_ipesquisa = ip(usuario_ipesquisa, senha_ipesquisa)
json_to_df = JD.JsonToDataFrame()

df_documents = api_ipesquisa.get_list_documents(dt_gravacao_inicio=date.today()-timedelta(days=10), dt_gravacao_fim=date.today())
df = json_to_df.get_dataframe3(df_documents)
#print(df[df['id']==4753])

######################## Atualização da tabela de i_pesquisa ########################################
processor= DF.DataFrameProcessor(Mysql, df, 'mydb.i_pesquisa')
processor.execute_insert_queries(vs)

######################## Atualização da tabela de projetos ########################################
api = api_manager.chamar_api(qp.query_pipe_projetos, vp.caminho_json_projetos)
projct = projeto_processor.update_database('myDB.projetos', 'idProjeto')