import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from io import StringIO


class IPesquisaAPI:
    def __init__(self, usuario, senha):
        self.base_url = 'https://sistema.ipesquisa.net/api/v1/pesquisa'
        self.auth = HTTPBasicAuth(usuario, senha)

    def get_list_documents(self, dt_gravacao_inicio=None, dt_gravacao_fim=None):
        url = self._build_url("list_documents", f'{self.base_url}/get-list-documents', dt_gravacao_inicio, dt_gravacao_fim)
        print(url)
        return self._make_request(url)

    def get_list_cases_by_id(self, id, dt_gravacao_inicio=None, dt_gravacao_fim=None):
        url = self._build_url("list_cases",f'{self.base_url}/{id}/get-list-cases', dt_gravacao_inicio, dt_gravacao_fim)
        return self._make_request(url)

    def get_csv_cases_by_id(self, id, dt_gravacao_inicio=None, dt_gravacao_fim=None):
        url = self._build_url("csv_cases",f'{self.base_url}/{id}/get-csv-cases', dt_gravacao_inicio, dt_gravacao_fim)
        print(url)
        return self._make_request(url, is_csv=True)

    def _build_url(self, tipo, base_url, dt_gravacao_inicio, dt_gravacao_fim):
        params = {}
        if dt_gravacao_inicio:
                if tipo == "csv_cases":
                    params['dt_gravacao_inicio'] = dt_gravacao_inicio
                elif tipo == "list_documents":
                    params['dt_cadastro_inicio'] = dt_gravacao_inicio
        if dt_gravacao_fim:
            if tipo == "csv_cases":
                params['dt_gravacao_fim'] = dt_gravacao_fim
            elif tipo == "list_documents":
                params['dt_cadastro_fim'] = dt_gravacao_fim
        return base_url + ('&' if '?' in base_url else '?') + '&'.join([f'{k}={v}' for k, v in params.items()])

    def _make_request(self, url, is_csv=False):
        response = requests.get(url, auth=self.auth)
        if response.status_code == 200:
            print("Request bem sucedido!")
            if is_csv:
                return pd.read_csv(StringIO(response.text),encoding='cp1252')
            else:
                return response.json()
        else:
            print(f"Erro no request: {response.status_code}")
            return None