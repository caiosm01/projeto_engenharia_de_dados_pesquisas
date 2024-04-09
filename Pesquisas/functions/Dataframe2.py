import pandas as pd
import regex

class DataFrameProcessor:
    """
    Classe responsável por processar um DataFrame para limpeza, normalização e preparação para inserção no banco de dados.
    """
    def __init__(self, Mysql, df, table_name, id_projeto, column_mappings=False):
        """
        Inicializa o DataFrameProcessor.

        :param Mysql: Conexão com o banco de dados.
        :param df: DataFrame a ser processado.
        :param table_name: Nome da tabela no banco de dados para inserção.
        :param id_projeto: ID do projeto para processamento.
        :param column_mappings: Mapeamentos opcionais para normalização de colunas.
        """
        self.Mysql = Mysql  # Conexão com o banco de dados.
        self.df = df  # DataFrame para processamento.
        self.table_name = table_name  # Nome da tabela no banco de dados.
        self.id_projeto = id_projeto  # ID do projeto.
        self.column_mappings = column_mappings  # Mapeamentos para normalização de colunas.
        self.base = self.fetch_base_data()  # Busca dados base do banco de dados.
        self.clean_and_filter_data()  # Realiza a limpeza e filtragem inicial do DataFrame.

    def fetch_base_data(self):
        """
        Busca dados base do banco de dados MySQL.

        :return: DataFrame com dados base.
        """
        # Retorna dados do banco de dados filtrados pelo mês atual.
        return self.Mysql.fetch_all("select * from mydb.pesquisas where MONTH(`Data_Inicio`) = month(current_date);")

    def strip_df(self):
        """
        Remove espaços em branco extras das strings em todas as colunas do DataFrame.
        """
        # Remove espaços em branco extras dos nomes das colunas.
        self.df.columns = self.df.columns.str.strip()
        for coluna in self.df.columns:
            try:
                # Remove espaços em branco extras dos valores das colunas.
                self.df[coluna] = self.df[coluna].str.strip()
                # Normaliza os valores das colunas.
                self.df[coluna] = self.df[coluna].str.normalize('NFC')
            except:
                pass  # Ignora erros em colunas que não são de texto.

    def remove_duplicates(self):
        """
        Remove linhas duplicadas do DataFrame.
        """
        # Remove duplicatas do DataFrame.
        self.df.drop_duplicates(inplace=True)

    def remover_emojis(self, coluna):
        """
        Remove emojis de uma coluna específica do DataFrame.

        :param coluna: Nome da coluna do DataFrame de onde os emojis serão removidos.
        """
        # Compila um padrão regex para identificar emojis.
        emoji_pattern = regex.compile("["
                                      u"\U0001F600-\U0001F64F"  # emoticons
                                      u"\U0001F300-\U0001F5FF"  # símbolos & pictogramas
                                      u"\U0001F680-\U0001F6FF"  # símbolos de transporte & mapas
                                      u"\U0001F1E0-\U0001F1FF"  # bandeiras (iOS)
                                      u"\U00002702-\U000027B0"  # dingbats
                                      u"\U000024C2-\U0001F251"  # símbolos diversos
                                      "]+", flags=regex.UNICODE)
        # Remove emojis da coluna especificada.
        if coluna in self.df.columns and pd.api.types.is_string_dtype(self.df[coluna]):
            self.df[coluna] = self.df[coluna].apply(lambda x: emoji_pattern.sub(r'', x) if isinstance(x, str) else x)
        else:
            print(f"Coluna '{coluna}' não encontrada ou não é do tipo string.")

    def process_data(self, sem_duplicadas=True):
        """
        Processa o DataFrame, aplicando limpeza e remoção de duplicatas.

        :param sem_duplicadas: Se verdadeiro, remove duplicatas do DataFrame.
        :return: DataFrame processado.
        """
        try:
            # Remove espaços em branco extras.
            self.strip_df()
            # Remove duplicatas, se necessário.
            if sem_duplicadas:
                self.remove_duplicates()
            return self.df
        except Exception as e:
            print(f"Erro ao processar DataFrame: {e}")


    def convert_to_date_or_datetime(self, col):
        """
        Converte uma coluna específica para data ou datetime, se possível.

        :param col: Nome da coluna para tentativa de conversão.
        """
        try:
            # Tenta converter a coluna para datetime
            temp_col = pd.to_datetime(self.df[col], errors='coerce')
            # Se todos os valores resultantes forem NaT, não realiza a conversão
            if temp_col.isna().all():
                return
            # Se a conversão for bem-sucedida, atualiza a coluna no DataFrame
            self.df[col] = temp_col
            # Verifica se a hora é sempre meia-noite e, se sim, converte para date
            notna_times = self.df[col].dropna()
            if notna_times.empty or not pd.api.types.is_datetime64_any_dtype(self.df[col]):
                return
            if ((notna_times.dt.hour == 0) & (notna_times.dt.minute == 0) & (notna_times.dt.second == 0)).all():
                self.df[col] = self.df[col].dt.date
        except (ValueError, TypeError):
            pass  # Se a conversão falhar, mantém a coluna como está

    def match_column_dtypes(self, df_old):
        """
        Alinha os tipos de dados entre o DataFrame atual e um DataFrame de referência.

        :param df_old: DataFrame de referência com os tipos de dados desejados.
        :return: DataFrame com tipos de dados alinhados.
        """
        for col in self.df.columns:
            if col in df_old.columns:
                try:
                    # Se a coluna de referência for do tipo 'object', tenta converter para date/datetime
                    if df_old[col].dtype == 'object':
                        self.convert_to_date_or_datetime(col)
                    # Converte a coluna para o tipo de dado da coluna de referência
                    self.df[col] = self.df[col].astype(df_old[col].dtype)
                except Exception as e:
                    print(f"Erro ao alinhar tipo de dados da coluna {col}: {e}")
        return self.df

    def generate_update_queries(self, df_old, coluna, table):
        """
        Gera queries de atualização SQL para sincronizar o DataFrame atual com um DataFrame de referência.

        :param df_old: DataFrame com os dados antigos.
        :param coluna: Coluna-chave para a junção dos DataFrames.
        :param table: Nome da tabela para a geração da query SQL.
        :return: Lista de strings com as queries de atualização.
        """
        try:
            # Alinha os nomes de colunas e tipos de dados com o DataFrame de referência
            self.df.columns = df_old.columns
            self.df = self.match_column_dtypes(df_old)
            # Une os DataFrames com base na coluna-chave
            df_unido = df_old.merge(self.df, on=coluna, suffixes=('_df1', '_df2'))
            queries = []
            # Gera as queries de atualização com base nas diferenças entre os DataFrames
            for index, row in df_unido.iterrows():
                for col in df_old.columns:
                    if col != coluna:
                        val_atual = row[col + '_df1']
                        val_novo = row[col + '_df2']
                        # Se os valores forem diferentes, cria uma query de atualização
                        if pd.isna(val_atual) and pd.isna(val_novo):
                            continue
                        if str(val_atual) != str(val_novo):
                            val_query = f"'{val_novo}'" if not pd.isna(val_novo) else 'NULL'
                            query = f"UPDATE {table} SET `{col}` = {val_query} WHERE `{coluna}`= '{row[coluna]}';"
                            queries.append(query)
            return queries
        except Exception as e:
            print(f"Erro ao gerar queries de atualização: {e}")
            return []

    def generate_insert_queries(self, table_name, colunas=None):
        """
        Gera queries de inserção SQL para adicionar novos registros do DataFrame a uma tabela.

        :param table_name: Nome da tabela para a geração da query SQL.
        :param colunas: Colunas específicas para inclusão na query (opcional).
        :return: Lista de tuples contendo a string da query e os valores a serem inseridos.
        """
        try:
            queries = []
            # Itera sobre cada linha do DataFrame para criar queries de inserção
            for index, row in self.df.iterrows():
                row = row.where(pd.notnull(row), None)  # Trata valores nulos
                values_placeholder = ("%s, " * (len(self.df.columns) - 1)) + "%s"
                # Cria a string da query de inserção
                if colunas:
                    query = f"INSERT INTO {table_name} ({colunas}) VALUES ({values_placeholder});"
                else:
                    query = f"INSERT INTO {table_name} VALUES ({values_placeholder});"
                queries.append((query, tuple(row)))
            return queries
        except Exception as e:
            print(f"Erro ao gerar queries de inserção: {e}")
            return []

    def process_data_2(self, sem_meta_dados=True, sem_duplicadas=True):
        """
        Processa o DataFrame realizando limpeza adicional e preparação para inserção.

        :param sem_meta_dados: Se verdadeiro, remove colunas de metadados.
        :param sem_duplicadas: Se verdadeiro, remove linhas duplicadas.
        :return: Indica se o DataFrame final tem linhas para processamento.
        """
        self.df = self.process_data(sem_duplicadas=sem_duplicadas)  # Processa o DataFrame
        self.df = self.df[self.df['Pesquisador'].str.strip() != 'Teste'].reset_index(drop=True)
        # Trata a presença de metadados conforme necessário
        if sem_meta_dados:
            self.df.drop(columns={'Pesquisador', 'Data Início', 'Data Fim', 'Latitude', 'Longitude'}, inplace=True)
        else:
            self.df['idProjeto'] = self.id_projeto
            self.df = self.df[
                ['idProjeto', 'Pesquisador', 'Nro. Identificação', 'Data Início', 'Data Fim', 'Latitude', 'Longitude']]
        # Retorna verdadeiro se houver linhas para processar
        return len(self.df) > 0

    def clean_and_filter_data(self):
        """
        Método abstrato para ser implementado nas subclasses para limpeza e filtragem específica de dados.
        """
        raise NotImplementedError

    def map_columns(self):
        """
        Mapeia colunas do DataFrame com base em um conjunto de mapeamentos fornecidos.

        Aplica normalização de dados nas colunas especificadas nos mapeamentos.
        """
        if self.column_mappings:
            for col, mapping in self.column_mappings.items():
                self.df[col] = self.df[col].str.capitalize().map(mapping)

    def execute_insert_queries(self, vs):
        """
        Executa queries de inserção geradas para o DataFrame.

        :param vs: Variáveis e configurações para geração de queries.
        :return: Resultado da execução das queries.
        """
        insert_queries = self.generate_insert_queries(self.table_name, colunas=vs.get_columns(self.table_name))
        for query in insert_queries:
            #print(query)  # Imprime a query
            result = self.Mysql.execute_query(query[0], query[1])  # Executa a query
            if result is not None:
                if len(result) > 0:
                    return result  # Retorna o resultado se houver algum



