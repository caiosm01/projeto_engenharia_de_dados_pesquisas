import queries_and_variables.variables_sac as vs
from functions import Dataframe as DF


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