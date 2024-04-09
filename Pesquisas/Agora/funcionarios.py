from functions import JasonDataframe as JD, Database as DB, Dataframe as DF
import queries_and_variables.variables_pipefy as vp
import queries_and_variables.queries_mysql as qm
import queries_and_variables.variables_sac as vs
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

    # def converter_data(self, data_str):
    #     for mes_pt, mes_num in meses.items():
    #         try:
    #             if mes_pt in data_str:
    #                 data_str = data_str.replace(mes_pt, mes_num)
    #                 data_str = data_str.replace(" de ", "/")
    #                 break
    #         except:
    #             pass
    #     return data_str

    def converter_data(self, data_str):
        try:
            return parse(data_str, dayfirst=True).strftime('%d/%m/%Y')
        except:
            return None



    def process_funcionarios_data(self):
        # Leitura do arquivo JSON e conversão para DataFrame
        json_to_DF = JD.JsonToDataFrame()
        data = json_to_DF.get_dataframe2(self.json_path)
        #data = pd.read_excel(r"C:\Users\Admin\Downloads\novo_relatório_06-03-2024.xlsx")

        # Lista com o nome das colunas esperadas
        expected_columns = ['ID', 'Status (Ativo | Inativo)', 'Nome Completo', "E-mail pessoal",
                            "E-mail institucional | @agorap.com.br", 'Telefone', "Cargo", "Data de nascimento",
                            "Estado Civil", "Escolaridade", "Sexo"]

        # Verificar se todas as colunas esperadas estão presentes no DataFrame
        missing_columns = [col for col in expected_columns if col not in data.columns]

        # Adicionar colunas ausentes com valores None
        for col in missing_columns:
            data[col] = None

        # data = data[['ID', 'Status (Ativo | Inativo)', 'Nome Completo', "E-mail pessoal",
        #          "E-mail institucional | @agorap.com.br", 'Telefone', "Cargo", "Data de nascimento", "Estado Civil",
        #          "Escolaridade", "Sexo"]]

        data = data[expected_columns]

        data['Telefone'] = data['Telefone'].str.split('+', expand=True)[1]
        data['Telefone'] = data['Telefone'].str.replace(r'\D', '', regex=True)
        data["Data de nascimento"] = data["Data de nascimento"].str.lower().apply(self.converter_data)
        data["Data de nascimento"] = pd.to_datetime(data["Data de nascimento"], format='%d/%m/%Y', errors='coerce')
        #print(data["Data de nascimento"])

        return data

    def update_database(self, table_name, id_column):
        try:
            # Conexão com o banco de dados
            self.Mysql.connect()

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
                #print(query)
                self.Mysql.execute_query(query)

            # Filtragem dos dados novos
            #print(data.columns)
            #new_data = data.loc[~data['idProjeto'].astype(str).isin(existing_data[id_column].astype(str))].reset_index(drop=True)
            # Geração de queries de inserção
            insert_queries = processor.generate_insert_queries(table_name, vs.funcionarios_columns)

            # Execução das queries de inserção
            for query, params in insert_queries:
                #print(query)
                self.Mysql.execute_query(query, params)

            # Fechamento da conexão com o banco de dados
            #self.Mysql.close()
            msg = f"Tabela de funcionarios atualizada com sucesso"
            print(msg)
            return msg
        except Exception as e:
            msg = f"Erro ao atualizar tabela de funcionarios: {e}"
            print(msg)
            return msg


'''if __name__ == "__main__":
    # Criação da instância com o pacote Database
    Mysql = DB.DatabaseConnection(host='127.0.0.1', database='banco_sac', user='root', password='#Agora123#')

    # Configurações para o processamento de projetos
    funcionarios_processor = DataFuncionariosProcessor(Mysql, vp.caminho_json_funcionarios)

    # Atualização da tabela de projetos no banco de dados
    funcionarios_processor.update_database('myDB.funcionarios', 'idFuncionario')
    Mysql.close()'''