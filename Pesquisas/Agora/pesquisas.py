import queries_and_variables.variables as vs
import numpy as np
from functions import Dataframe as DF
import pandas as pd



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




def pesquisas(Mysql, df, vs, id_projeto, id_questionario):
    """
    Processa e insere dados de pesquisas no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados das pesquisas.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de pesquisas
    pesquisa_processor = PesquisaProcessor(Mysql, df, 'mydb.pesquisas', id_projeto, id_questionario)
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