import json
import requests
import os
from dotenv import load_dotenv
from functions import Database as DB
import queries_and_variables.variables_pipefy as vp

load_dotenv(override=True)

class ApiManager:
    def __init__(self, Mysql):
        """
        Inicializa a instância da classe ApiManager.

        :param Mysql: Instância do DatabaseConnection para interagir com o banco de dados.
        """
        self.token = os.getenv("token")
        self.url = os.getenv("url")
        self.Mysql = Mysql

    def verificar_limite_api(self):
        """
        Verifica se o limite de chamadas da API foi atingido.

        :raises Exception: Exceção é levantada se o limite de chamadas da API for atingido.
        """
        dados = self.Mysql.fetch_all("SELECT * FROM api_pipefy.api_calls WHERE id = 1;")
        chamadas = dados['calls_made'][0]
        if chamadas >= dados['calls_limit'][0]:
            raise Exception("Limite de chamadas da API atingido!")
        else:
            print(f"Chamadas realizadas: {chamadas}/{dados['calls_limit'][0]}")

    def verificar_limite_api_dia(self):
        """
        Verifica se o limite de chamadas diárias da API foi atingido.

        :raises Exception: Exceção é levantada se o limite de chamadas diárias da API for atingido.
        """
        dados = self.Mysql.fetch_all("SELECT * FROM api_pipefy.api_calls WHERE id = 1;")
        chamadas = dados['calls_made_day'][0]
        if chamadas >= dados['calls_limit_day'][0]:
            raise Exception("Limite de chamadas da API diário atingido!")
        else:
            print(f"Chamadas realizadas: {chamadas}/{dados['calls_limit_day'][0]}")

    def verificar_limite_api_hora(self):
        """
        Verifica se o limite de chamadas por hora da API foi atingido.

        :raises Exception: Exceção é levantada se o limite de chamadas por hora da API for atingido.
        """
        dados = self.Mysql.fetch_all("SELECT * FROM api_pipefy.api_calls WHERE id = 1;")
        chamadas = dados['calls_made_hour'][0]
        if chamadas >= dados['calls_limit_hour'][0]:
            raise Exception("Limite de chamadas da API por hora atingido!")
        else:
            print(f"Chamadas realizadas: {chamadas}/{dados['calls_limit_hour'][0]}")

    def registrar_chamada_api(self):
        """
        Registra uma chamada à API no banco de dados.
        """
        self.Mysql.execute_query("UPDATE api_pipefy.api_calls SET calls_made = calls_made + 1 WHERE id = 1")
        self.Mysql.execute_query("UPDATE api_pipefy.api_calls SET calls_made_day = calls_made_day + 1 WHERE id = 1")
        self.Mysql.execute_query("UPDATE api_pipefy.api_calls SET calls_made_hour = calls_made_hour + 1 WHERE id = 1")

    def chamar_api(self, query_pipe, arquivo):
        """
        Realiza uma chamada à API do Pipefy.

        :param query_pipe: A query a ser enviada para a API.
        :param arquivo: O nome do arquivo onde os dados da API serão salvos.
        :return: Os dados retornados pela API.
        """
        try:
            self.verificar_limite_api()
            self.verificar_limite_api_dia()
            self.verificar_limite_api_hora()

            headers = {
                'Authorization': f"Bearer {self.token}",
                'Content-Type': "application/json"
            }

            response = requests.post(
                self.url,
                json={"query": query_pipe},
                headers=headers
            )

            self.registrar_chamada_api()

            dados = json.loads(response.text)
            with open(arquivo, 'w') as file:
                json.dump(dados, file)

            # Pode adicionar mais código aqui para processar 'dados' ou salvar no banco de dados
            msg = f"API do pipefy, para {arquivo}, foi bem sucedida"
            print(msg)
            return msg

        except Exception as e:
            msg = f'Erro com a api do pipefy: {e}'
            print(msg)
            return msg




# query_pipe = """{allCards(pipeId:303834641)
#   {edges
#     {
#       node{
#         current_phase{
#           name}
#         id
#         title
#         fields
#         {name
#           value
#         }
#       }
#     }
#     }
#   }"""
#
#
# Mysql = DB.DatabaseConnection(host='127.0.0.1', database='banco_sac', user='root', password='#Agora123#')
# Mysql.connect()
# api_manager = ApiManager(Mysql)
# try:
#     dados_api = api_manager.chamar_api(query_pipe, vp.caminho_json_projetos)
# except Exception as e:
#     print(e)