import queries_and_variables.variables_sac as vs
import numpy as np
from functions import Dataframe as DF


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