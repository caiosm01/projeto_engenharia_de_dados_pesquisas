from functions import Database as db, Dataframe as df

Consulta = "C:/Users/Admin/Downloads/informações_04-01-2024 (1).xlsx"
base = db.DatabaseConnection(host='127.0.0.1', database='banco_sac', user='root', password='#Agora123#')
base.connect()

###################################################### Atualização da tabela coordenador ###############################################################################################################################

supervisor = base.fetch_all("SELECT * FROM mydb.supervisor;")
funcionarios = base.fetch_all("SELECT * FROM mydb.funcionarios;")

colunas = ['Código', 'Supervisor de Campo']

data = df.DataFrameProcessor.load_excel(Consulta, colunas)

for i in range(len(data["Supervisor de Campo"].str.split(',',expand=True).columns)):
    data["Supervisor de Campo_"+f"col_{i}"] = data["Supervisor de Campo"].str.split(',',expand=True)[i]

data.drop(columns={'Supervisor de Campo'}, inplace=True)

data = data.melt(id_vars=['Código'],var_name='Colunas', value_name='Supervisor de Campo')
data.drop(columns={'Colunas'}, inplace=True)

data.rename(columns={'Supervisor de Campo': 'Nome'}, inplace=True)
data = data.merge(funcionarios[['idFuncionario', 'Nome']], how='left', on='Nome')
data = data[['Código', 'idFuncionario']]

base.execute_query("SET SQL_SAFE_UPDATES = 0;")
base.execute_query("""
DELETE S
FROM mydb.supervisor S
LEFT JOIN mydb.projetos P ON S.idProjeto = P.idProjeto
WHERE YEAR(P.`inicio_projeto`) = YEAR(CURRENT_DATE());
""")
base.execute_query("SET SQL_SAFE_UPDATES = 1;")

insert_queries = df.DataFrameProcessor.generate_insert_queries(data, 'mydb.supervisor')
for query, params in insert_queries:
    base.execute_query(query, params)

# Fechar a conexão com o banco de dados
base.close()