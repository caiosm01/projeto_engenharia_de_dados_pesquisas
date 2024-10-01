from functions import JasonDataframe as jd, Database as db, Dataframe as df
import queries_and_variables.variables_pipefy as vp
################################################### INSTÂNCIAS ###################################################
# Criação da instância com o pacote Database
Mysql = db.DatabaseConnection(host='127.0.0.1', database='banco_sac', user='root', password='#Agora123#')

# Criação da instância com o pacote JsonToDataFrame, que ocnverte arquivos em json para dataframes
json_to_df = jd.JsonToDataFrame()

processor = df.DataFrameProcessor()
###################################################################################################################

# Conexão com o banco de dados
Mysql.connect()

###################################################### Atualização da tabela coordenador ###############################################################################################################################

coordenador = Mysql.fetch_all("SELECT * FROM mydb.coordenador;")
funcionarios = Mysql.fetch_all("SELECT * FROM mydb.funcionarios;")

colunas = ['Código', 'Coordenador do Projeto']

#data = df.DataFrameProcessor.load_excel(Consulta, colunas)
data = json_to_df.get_dataframe(vp.caminho_json_projetos, vp.field_mapping_coordenador_projetos)

#data.rename(columns={'Coordenador do Projeto': 'Nome'}, inplace=True)

data = data.merge(funcionarios[['idFuncionario', 'Nome']], on='Nome', how='left')
data = data[['Código', 'idFuncionario']]

update_queries = df.DataFrameProcessor.generate_update_queries(data, coordenador, 'idProjeto', 'coordenador')
for query in update_queries:
    print(query)
    Mysql.execute_query(query)

# Gerar e executar queries_and_variables de inserção
df_novos = data.loc[~data['Código'].astype(str).isin(coordenador['idProjeto'].astype(str))].reset_index(drop=True)
insert_queries = df.DataFrameProcessor.generate_insert_queries(df_novos, 'mydb.coordenador')
for query, params in insert_queries:
    Mysql.execute_query(query, params)

# Fechar a conexão com o banco de dados
Mysql.close()