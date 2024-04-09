import pandas as pd
import numpy as np
from functions import Dataframe as DF
from datetime import date
from dotenv import load_dotenv
import os

load_dotenv(override=True)
Escala_meta = os.getenv("Escala_meta")



class MetaProcessor(DF.DataFrameProcessor):
    """
    Classe para processar dados de escala dos pesquisadores.
    Herda de DF.DataFrameProcessor para utilizar suas funcionalidades de processamento de DataFrame.
    """
    def clean_and_filter_data(self):
        """
        Limpa e filtra os dados do DataFrame específico para escala dos pesquisadores.
        """
        pesquisadores = pd.read_excel(Escala_meta, sheet_name='Base_AUX', header=0)

        self.df = self.df[['Aeroporto', 'Pesquisador', 'Tipo de Voo', 'Procedimento', 'Sub-Procedimento', 'CIA', 'META']]
        self.process_data()
        self.df['Sub-Procedimento'] = self.df['Sub-Procedimento'].replace('Check-In', 'Check In')
        self.df['Sub-Procedimento'] = self.df['Sub-Procedimento'].str.capitalize()

        rename_dict = {
            'Sub-Procedimento': 'Processo',
            'Tipo de Voo': 'Estrato',
            'CIA': 'Companhia',
        }

        # Renomeia as colunas conforme o dicionário
        self.df.rename(columns=rename_dict, inplace=True)

        # Aplica mapeamentos de colunas, se definidos
        self.map_columns()

        self.df = pd.merge(self.df, pesquisadores[['Pesquisador', 'ID PIPE']], how='left', on='Pesquisador')
        self.df = self.df.drop(['Pesquisador'], axis=1)

        self.df = self.df.rename(columns={'ID PIPE': 'Pesquisador'})

        self.df['META'] = pd.to_numeric(self.df['META'].replace(np.nan, 0), downcast="integer")

        self.df['META'] = self.df['META'].round(0)

        self.df = self.df[['Pesquisador', 'Aeroporto', 'Processo', 'Estrato', 'Companhia', 'Procedimento', 'META']]

        self.df = self.df[pd.isna(self.df['Pesquisador']) == False]
        self.df = self.df[pd.isna(self.df['Aeroporto']) == False]

        self.df['Mês'] = str(date.today().month)
        self.df['Ano'] = str(date.today().year)
        self.df["Companhia"] = self.df["Companhia"].astype(str).replace('nan', 4)

        self.Mysql.execute_query("SET SQL_SAFE_UPDATES = 0;")
        self.Mysql.execute_query("DELETE IGNORE FROM banco_sac2.meta_pesquisadores where `Ano` = year(current_date()) and `Mês` =month(current_date());")
        self.Mysql.execute_query("SET SQL_SAFE_UPDATES = 1;")


def Meta(Mysql, df, vs):
    """
    Processa e insere dados de escala dos pesquisadores no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados de escala dos pesquisadores.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de satisfação
    meta_processor = MetaProcessor(Mysql, df, 'banco_sac2.meta_pesquisadores', column_mappings=vs.dict_mappings)
    # Executa as queries de inserção
    result = meta_processor.execute_insert_queries(vs)
    return result