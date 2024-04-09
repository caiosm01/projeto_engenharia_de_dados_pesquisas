import pandas as pd
from functions import Dataframe as DF
from datetime import date
from dotenv import load_dotenv
import os

load_dotenv(override=True)
Escala_meta = os.getenv("Escala_meta")

class EscalaProcessor(DF.DataFrameProcessor):
    """
    Classe para processar dados de escala dos pesquisadores.
    Herda de DF.DataFrameProcessor para utilizar suas funcionalidades de processamento de DataFrame.
    """
    def clean_and_filter_data(self):
        """
        Limpa e filtra os dados do DataFrame específico para escala dos pesquisadores.
        """
        pesquisadores = pd.read_excel(Escala_meta, sheet_name='Base_AUX', header=0)

        self.df = self.df[['Aeroporto', 'Pesquisador', '1', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13','14',
             '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']]
        #print(self.df)
        self.process_data()
        self.df = pd.melt(self.df.reset_index(), id_vars=['Aeroporto', 'Pesquisador'],
                            value_vars=['1', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13','14',
                                        '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27','28',
                                        '29', '30', '31'],
                            var_name='Data', value_name='Índice')
        self.df = self.df[self.df['Índice'] == 1]

        self.df['Data'] = pd.to_datetime(self.df['Data'] + "/" + str(date.today().month) + "/" + str(date.today().year),format='%d/%m/%Y')

        # Aplica mapeamentos de colunas, se definidos
        self.map_columns()
        self.df = pd.merge(self.df, pesquisadores[['Pesquisador', 'ID PIPE']], how='left', on='Pesquisador')
        self.df = self.df.drop(['Pesquisador', 'Índice'], axis=1)

        self.df = self.df.rename(columns={'ID PIPE': 'Pesquisador'})

        self.df = self.df[['Pesquisador', 'Aeroporto', 'Data']]

        self.df = self.df[pd.isna(self.df['Pesquisador']) == False]
        self.df = self.df[pd.isna(self.df['Aeroporto']) == False]

        self.Mysql.execute_query("SET SQL_SAFE_UPDATES = 0;")
        self.Mysql.execute_query("DELETE FROM banco_sac2.escala_pesquisadores where year(`Data`) = year(current_date()) and month(`Data`) =month(current_date());")
        self.Mysql.execute_query("SET SQL_SAFE_UPDATES = 1;")
        print(self.df)


def Escala(Mysql, df, vs):
    """
    Processa e insere dados de escala dos pesquisadores no banco de dados.

    :param Mysql: Conexão com o banco de dados.
    :param df: DataFrame contendo os dados de escala dos pesquisadores.
    :param vs: Variáveis e configurações específicas para a inserção.
    :param id_projeto: ID do projeto para processamento dos dados.
    """
    # Cria uma instância do processador de satisfação
    escala_processor = EscalaProcessor(Mysql, df, 'banco_sac2.escala_pesquisadores', column_mappings=vs.dict_mappings)
    # Executa as queries de inserção
    result = escala_processor.execute_insert_queries(vs)
    return result
