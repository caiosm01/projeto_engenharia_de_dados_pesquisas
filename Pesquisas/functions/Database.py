import mysql.connector
import pandas as pd
from mysql.connector import Error

class DatabaseConnection:
    """
    Classe para gerenciar a conexão com o banco de dados MySQL.
    Permite conectar, executar consultas, buscar dados e fechar a conexão.
    """
    def __init__(self, host, database, user, password):
        """
        Inicializa a conexão com o banco de dados.

        :param host: Endereço do host do banco de dados MySQL.
        :param database: Nome do banco de dados a ser utilizado.
        :param user: Nome do usuário para acesso ao banco de dados.
        :param password: Senha para acesso ao banco de dados.
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Estabelece uma conexão com o banco de dados MySQL.

        Levanta uma exceção em caso de falha na conexão.
        """
        try:
            # Cria uma conexão com o banco de dados
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            # Cria um cursor para executar consultas
            self.cursor = self.connection.cursor()
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            raise

    def execute_query(self, query, values=None, params=None):
        """
        Executa uma consulta SQL no banco de dados.

        :param query: A consulta SQL a ser executada.
        :param values: Valores para a consulta (usado em consultas parametrizadas).
        :param params: Parâmetros adicionais para a consulta.
        :return: Lista vazia em caso de sucesso, ou lista com erro e código de erro em caso de falha.
        """
        try:
            # Executa a consulta com os valores e parâmetros fornecidos
            self.cursor.execute(query, values, params)
            # Faz commit das mudanças no banco de dados
            self.connection.commit()
            return "dado inserido no banco"
        except Error as e:
            # Ignora erros específicos ou imprime outros
            if e.errno == 1062:  # Erro de duplicata, pode ser ignorado
                return "duplicado"
                #pass
            else:
                print(f"Erro ao executar a query: {e}")
                return [e, e.errno]

    def fetch_all(self, query, params=None):
        """
        Executa uma consulta SQL e retorna todos os dados recuperados.

        :param query: A consulta SQL a ser executada.
        :param params: Parâmetros adicionais para a consulta.
        :return: DataFrame contendo todos os dados recuperados.
        """
        try:
            # Executa a consulta e recupera os dados
            self.cursor.execute(query, params)
            # Cria um DataFrame com os dados recuperados
            return pd.DataFrame(self.cursor.fetchall(), columns=self.cursor.column_names)
        except Error as e:
            print(f"Erro ao buscar dados: {e}")
            raise

    def close(self):
        """
        Fecha o cursor e a conexão com o banco de dados.
        """
        # Fecha o cursor se estiver aberto
        if self.cursor:
            self.cursor.close()
        # Fecha a conexão se estiver aberta
        if self.connection:
            self.connection.close()
