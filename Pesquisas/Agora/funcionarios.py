from functions import JasonDataframe as JD, Database as DB, Dataframe as DF
import queries_and_variables.variables_pipefy as vp
import queries_and_variables.queries_mysql as qm
import queries_and_variables.variables as vs
import pandas as pd
import calendar
import locale
from dateutil.parser import parse
locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')

meses = {v: str(k).zfill(2) for k,v in enumerate(calendar.month_name[1:], 1)}

class DataFuncionariosProcessor:
    def __init__(self, Mysql, json_path):
        self.Mysql = Mysql
        self.json_path = json_path


    def converter_data(self, data_str):
        try:
            return parse(data_str, dayfirst=True).strftime('%d/%m/%Y')
        except:
            return None



    def process_funcionarios_data(self):
        # Leitura do arquivo JSON e conversão para DataFrame
        json_to_DF = JD.JsonToDataFrame()
        data = json_to_DF.get_dataframe2(self.json_path)

        # Lista com o nome das colunas esperadas
        expected_columns = ['ID', 'Status', 'Nome Completo', "E-mail pessoal",
                            "E-mail institucional", 'Telefone', "Cargo", "Data de nascimento",
                            "Estado Civil", "Escolaridade", "Sexo"]

        # Verificar se todas as colunas esperadas estão presentes no DataFrame
        missing_columns = [col for col in expected_columns if col not in data.columns]

        # Adicionar colunas ausentes com valores None
        for col in missing_columns:
            data[col] = None

        data = data[expected_columns]

        data['Telefone'] = data['Telefone'].str.split('+', expand=True)[1]
        data['Telefone'] = data['Telefone'].str.replace(r'\D', '', regex=True)
        data["Data de nascimento"] = data["Data de nascimento"].str.lower().apply(self.converter_data)
        data["Data de nascimento"] = pd.to_datetime(data["Data de nascimento"], format='%d/%m/%Y', errors='coerce')

        return data

    def update_database(self, table_name, id_column):
        try:

            # Leitura da tabela existente no banco de dados
            existing_data = self.Mysql.fetch_all(qm.mydb_funcionarios)
            existing_data = existing_data[["idFuncionario", "Status", "Nome", "email_pessoal", "email_institucional", "Telefone", "cargo", "data_nasc", "estado_civil", "escolaridade", "sexo"]]

            # Processamento dos dados
            data = self.process_funcionarios_data()

            processor = DF.DataFrameProcessor(df=data)
            data = processor.process_data()
            # Geração de queries de atualização
            update_queries = processor.generate_update_queries(existing_data, id_column, table_name)
            # Execução das queries de atualização
            for query in update_queries:
                self.Mysql.execute_query(query)

            insert_queries = processor.generate_insert_queries(table_name, vs.funcionarios_columns)

            # Execução das queries de inserção
            for query, params in insert_queries:
                self.Mysql.execute_query(query, params)

            msg = f"Tabela de funcionarios atualizada com sucesso"
            print(msg)
            return msg
        except Exception as e:
            msg = f"Erro ao atualizar tabela de funcionarios: {e}"
            print(msg)
            return msg