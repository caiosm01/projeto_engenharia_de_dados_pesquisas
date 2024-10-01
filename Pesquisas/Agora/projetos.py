from functions import JasonDataframe as JD, Database as DB, Dataframe as DF
import queries_and_variables.variables_pipefy as vp
import queries_and_variables.queries_mysql as qm
import queries_and_variables.variables as vs

class DataProjetosProcessor:
    def __init__(self, Mysql, json_path, field_mapping):
        self.Mysql = Mysql
        self.json_path = json_path
        self.field_mapping = field_mapping

    def process_projetos_data(self):
        # Leitura do arquivo JSON e conversão para DataFrame
        json_to_DF = JD.JsonToDataFrame()
        data = json_to_DF.get_dataframe(self.json_path, self.field_mapping)

        # Mapeamento da coluna "fase_atual"
        data['fase_atual'] = data['fase_atual'].map(vp.fases_pipe_projetos)

        return data

    def update_database(self, table_name, id_column):
        try:

            # Leitura da tabela existente no banco de dados
            existing_data = self.Mysql.fetch_all(qm.mydb_projetos)

            # Processamento dos dados
            data = self.process_projetos_data()

            processor = DF.DataFrameProcessor(df=data)
            data = processor.process_data()
            # Geração de queries de atualização
            update_queries = processor.generate_update_queries(existing_data, id_column, table_name)
            
            # Execução das queries de atualização
            for query in update_queries:
                self.Mysql.execute_query(query)

            insert_queries = processor.generate_insert_queries(table_name, vs.projetos_columns)

            # Execução das queries de inserção
            for query, params in insert_queries:
                self.Mysql.execute_query(query, params)

            # Fechamento da conexão com o banco de dados
            msg = f"Tabela de projetos atualizada com sucesso"
            return msg
        
        except Exception as e:
            msg = f"Erro ao atualizar tabela de projetos: {e}"
            return msg