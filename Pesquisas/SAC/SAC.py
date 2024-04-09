import queries_and_variables.variables_sac as vs
import numpy as np
from functions import Dataframe as DF, Database as DB
import pandas as pd

# Estabelece conexão com o banco de dados MySQL
#Mysql = DB.DatabaseConnection(host='127.0.0.1', database='banco_sac', user='root', password='#Agora123#')
#Mysql.connect()

# Lê um arquivo CSV para um DataFrame pandas
# df = pd.read_csv("path_to_csv_file", encoding='cp1252')  # Descomente e substitua o caminho para o arquivo CSV real
#df = pd.read_csv("C:/Users/Admin/OneDrive - agorap.com.br/Documentos/CSV2/#847729694 - SAC _ Medição _ Janeiro 2024.csv", encoding='cp1252')  # Exemplo de caminho de arquivo

class SatisfacaoProcessor(DF.DataFrameProcessor):
    """
    Classe para processar dados de satisfação dos clientes.
    Herda de DF.DataFrameProcessor para utilizar suas funcionalidades de processamento de DataFrame.
    """
    def clean_and_filter_data(self):
        """
        Limpa e filtra os dados do DataFrame específico para satisfação dos clientes.
        """
        # Processa os dados e retorna se não houver dados a processar
        if not self.process_data_2():
            return len(self.df)

        # Dicionário para renomear colunas
        rename_dict = {
            'L1 - Aeroporto onde foi entrevistado. R.U (PESQUISADOR)': 'Aeroporto',
            'L8-  Processo. R.U (PESQUISADOR)': 'Processo',
            'L7- Tipo de Voo. R.U  (PESQUISADOR)': 'Estrato'
        }
        # Renomeia as colunas conforme o dicionário
        self.df.rename(columns=rename_dict, inplace=True)

        # Processa colunas específicas para extrair e limpar dados
        self.df['Aeroporto'] = self.df['Aeroporto'].str.split('-', expand=True)[0].str.strip()
        self.df['L5 - Terminal de embarque ou desembarque de realização da entrevista. (Pesquisador)'] = self.df['L5 - Terminal de embarque ou desembarque de realização da entrevista. (Pesquisador)'].str.split(' ', expand=True)[1].str.strip()
        self.df['L9- Por qual companhia aérea o senhor viajou ou vai viajar? R.U (ESPONTÂNEA)'] = self.df['L9- Por qual companhia aérea o senhor viajou ou vai viajar? R.U (ESPONTÂNEA)'].str.capitalize()
        self.df.loc[(self.df["L9- Por qual companhia aérea o senhor viajou ou vai viajar? R.U (ESPONTÂNEA)"] == "Outra"), 'L9- Por qual companhia aérea o senhor viajou ou vai viajar? R.U (ESPONTÂNEA)'] = self.df["L9.a - Outra qual?"]
        self.df.replace(vs.avaliacoes, inplace=True)

        # Aplica mapeamentos de colunas, se definidos
        self.map_columns()



class SatisfacaoInglesProcessor(DF.DataFrameProcessor):
    """
    Classe para processar dados de satisfação dos clientes.
    Herda de DF.DataFrameProcessor para utilizar suas funcionalidades de processamento de DataFrame.
    """
    def clean_and_filter_data(self):
        """
        Limpa e filtra os dados do DataFrame específico para satisfação dos clientes.
        """
        # Processa os dados e retorna se não houver dados a processar
        if not self.process_data_2():
            return len(self.df)

        # Dicionário para renomear colunas
        rename_dict = {
            'L1 - Aeroporto onde foi entrevistado. R.U (PESQUISADOR)': 'Aeroporto',
            'L8-  Processo. R.U (PESQUISADOR)': 'Processo',
            'L7- Tipo de Voo. R.U  (PESQUISADOR)': 'Estrato'
        }
        # Renomeia as colunas conforme o dicionário
        self.df.rename(columns=rename_dict, inplace=True)

        # Processa colunas específicas para extrair e limpar dados
        self.df['Aeroporto'] = self.df['Aeroporto'].str.split('-', expand=True)[0].str.strip()
        self.df['L5 - Terminal de embarque ou desembarque de realização da entrevista. (Pesquisador)'] = self.df['L5 - Terminal de embarque ou desembarque de realização da entrevista. (Pesquisador)'].str.split(' ', expand=True)[1].str.strip()
        self.df['L9 - Which airline did you travel or will you travel with?'] = self.df['L9 - Which airline did you travel or will you travel with?'].str.capitalize()
        self.df.loc[(self.df['L9 - Which airline did you travel or will you travel with?'] == "Outra"), 'L9 - Which airline did you travel or will you travel with?'] = self.df["L9.a - Another one, which one?"]
        self.df.replace(vs.avaliacoes, inplace=True)

        # Aplica mapeamentos de colunas, se definidos
        self.map_columns()



class MedicaoProcessor(DF.DataFrameProcessor):
    """
    Classe para processar dados de medição.
    Herda de DF.DataFrameProcessor para utilizar suas funcionalidades de processamento de DataFrame.
    """
    def clean_and_filter_data(self):
        """
        Limpa e filtra os dados do DataFrame específico para medição.
        """
        # Processa os dados sem remover duplicatas e retorna se não houver dados a processar
        if not self.process_data_2(sem_duplicadas=False):
            return len(self.df)

        # Dicionário para renomear colunas
        rename_dict = {
            '1- Aeroporto': 'Aeroporto',
            '3- Estrato': 'Estrato',
            '2- Processo': 'Processo'
        }
        # Renomeia as colunas conforme o dicionário
        self.df.rename(columns=rename_dict, inplace=True)

        # Processa colunas específicas para extrair e limpar dados
        self.df = self.df.drop(columns=['Nome e sobrenome do pesquisador que não consta na lista.'])
        self.df["Aeroporto"] = self.df["Aeroporto"].str.split('-', expand=True)[0].str.strip()
        self.df.loc[(self.df["4- Companhia aérea?"] == "Outros"), '4- Companhia aérea?'] = self.df["4.1 - OUTROS:"]
        self.df['4- Companhia aérea?'] = self.df['4- Companhia aérea?'].str.capitalize()
        self.df["Estrato"] = self.df["Estrato"].replace([np.nan,None], 'INTERNACIONAL')

        # Aplica mapeamentos de colunas, se definidos
        self.map_columns()

class PesquisaProcessor(DF.DataFrameProcessor):
    """
    Classe para processar dados de pesquisa.
    Herda de DF.DataFrameProcessor para utilizar suas funcionalidades de processamento de DataFrame.
    """
    @staticmethod
    def corrigir_latitude(lat):
        """
        Corrige e formata valores de latitude.

        :param lat: Valor da latitude a ser corrigido.
        :return: Valor de latitude corrigido.
        """
        lat_str = str(lat)  # Converte o valor para string

        # Trata os casos de dados ausentes ou inválidos
        if lat_str == '0' or lat_str.startswith('-999'):
            return None  # Retorna None para dados ausentes/inválidos

        # Remove '.0' se presente no final do valor
        if lat_str.endswith('.0'):
            lat_str = lat_str[:-2]

        # Insere o ponto decimal, se necessário
        if '.' not in lat_str:
            if len(lat_str) > 8:
                return lat_str[:-8] + '.' + lat_str[-8:]
            else:
                return '0.' + lat_str
        else:
            return lat_str  # Retorna o valor se o ponto decimal já estiver na posição correta

    def clean_and_filter_data(self):
        """
        Limpa e filtra os dados do DataFrame específico para pesquisa.
        """
        # Processa os dados sem remover metadados e retorna se não houver dados a processar
        if not self.process_data_2(sem_meta_dados=False):
            return len(self.df)

        # Processa colunas específicas para extrair e limpar dados
        self.df["Pesquisador"] = self.df["Pesquisador"].str.split('|', expand=True)[1]
        self.df['Pesquisador'] = self.df['Pesquisador'].replace(['Outro', None, np.nan], 9191)


        # Converte as datas para o formato datetime
        self.df['Data Início'] = pd.to_datetime(self.df['Data Início'], format='%d/%m/%Y %H:%M:%S')
        self.df['Data Fim'] = pd.to_datetime(self.df['Data Fim'], format='%d/%m/%Y %H:%M:%S')

        # Aplica a correção de latitude e longitude
        self.df['Latitude'] = self.df['Latitude'].apply(PesquisaProcessor.corrigir_latitude)
        self.df['Longitude'] = self.df['Longitude'].apply(PesquisaProcessor.corrigir_latitude)




def satisfacao(Mysql, df, vs, id_projeto):
    """
    Processa e insere dados de satisfação no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados de satisfação.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de satisfação
    satisfacao_processor = SatisfacaoProcessor(Mysql, df, 'banco_sac2.satisfacao', id_projeto, column_mappings=vs.dict_mappings)
    # Executa as queries de inserção
    result = satisfacao_processor.execute_insert_queries(vs)

    if 'dados inseridos:' in str(result):
        msg = f"{result}"
        print(msg)
        return [0, msg]
    elif result[0][1] == 1452:
        if 'fk_satisfacao_pesquisas1' in str(result[0][0]):
            msg = f"Erro - fk_satisfacao_pesquisas1\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [1, msg]
        else:
            msg = f"Erro de chave estrangeira com outra tabela: {result[0][0]}\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [2, msg]
    else:
        msg = "Erro não identificado"
        print(msg)
        return [3, msg]


def satisfacao_ingles(Mysql, df, vs, id_projeto):
    """
    Processa e insere dados de satisfação no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados de satisfação.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de satisfação
    satisfacao_processor = SatisfacaoInglesProcessor(Mysql, df, 'banco_sac2.satisfacao', id_projeto, column_mappings=vs.dict_mappings)
    # Executa as queries de inserção
    result = satisfacao_processor.execute_insert_queries(vs)
    if 'dados inseridos:' in str(result):
        msg = f"{result}"
        print(msg)
        return [0, msg]
    elif result[0][1] == 1452:
        if 'fk_satisfacao_pesquisas1' in str(result[0][0]):
            msg = f"Erro - fk_satisfacao_pesquisas1\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [1, msg]
        else:
            msg = f"Erro de chave estrangeira com outra tabela: {result[0][0]}\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [2, msg]
    else:
        msg = "Erro não identificado"
        print(msg)
        return [3, msg]



def medicao(Mysql, df, vs, id_projeto):
    """
    Processa e insere dados de medição no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados de medição.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de medição
    medicao_processor = MedicaoProcessor(Mysql, df, 'banco_sac2.medicao', id_projeto, column_mappings=vs.dict_mappings)
    # Executa as queries de inserção
    result = medicao_processor.execute_insert_queries(vs)

    if 'dados inseridos:' in str(result):
        msg = f"{result}"
        print(msg)
        return [0, msg]
    elif result[0][1] == 1452:
        if 'fk_medicao_pesquisas1' in str(result[0][0]):
            msg = f"Erro - fk_medicao_pesquisas1\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [1, msg]
        else:
            msg = f"Erro de chave estrangeira com outra tabela: {result[0][0]}\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [2, msg]
    else:
        msg = "Erro não identificado"
        print(msg)
        return [3, msg]


def pesquisas(Mysql, df, vs, id_projeto):
    """
    Processa e insere dados de pesquisas no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados das pesquisas.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de pesquisas
    pesquisa_processor = PesquisaProcessor(Mysql, df, 'mydb.pesquisas', id_projeto)
    # Executa as queries de inserção
    result = pesquisa_processor.execute_insert_queries(vs)
    #print(result)
    if 'dados inseridos:' in str(result):
        msg = f"{result}"
        print(msg)
        return [0, msg]
    elif result[0][1] == 1452:
        if 'fk_Pesquisas_Projetos1' in str(result[0][0]):
            msg = f"Erro - fk_Pesquisas_Projetos1\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [1, msg]
        elif 'fk_Pesquisas_Funcionarios1' in str(result[0][0]):
            msg = f"Erro - fk_Pesquisas_Funcionarios1\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [2, msg]
        else:
            msg = f"Erro de chave estrangeira com outra tabela: {result[0][0]}\nQuery: {result[1]}\nDados para serem inseridos: {result[2]}\nDados que foram inseridos: {result[2]}"
            print(msg)
            return [3, msg]
    else:
        msg = "Erro não identificado"
        print(msg)
        return [4, msg]

