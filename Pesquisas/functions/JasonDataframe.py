import json
import pandas as pd

class JsonToDataFrame:
    def __init__(self):
        self.data = None
        self.df = None

    def load_json(self, filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as arquivo:
                self.data = json.load(arquivo)
        except json.JSONDecodeError:
            print("Erro ao ler o arquivo JSON.")
            self.data = {}
        except:
            self.data = filepath

    def check_errors(self):
        try:
            if self.data.get("errors"):
                print("Erro no JSON:", self.data["errors"])
                return False
            return True
        except:
            pass

    def process_data(self, field_mapping):
        lista_de_series = []

        for card in self.data['data']['allCards']['edges']:
            node = card.get('node', {})
            fase_atual = node.get('current_phase', {}).get('name')
            campos = {campo['name']: campo['value'] for campo in node.get('fields', [])}

            # Dados comuns
            dados_comuns = {
                "title": node.get('title'),
                "id": node.get('id'),
                "fase_atual": fase_atual,
            }

            # Dados específicos
            dados_especificos = {mapped_field: campos.get(original_field)
                                 for original_field, mapped_field in field_mapping.items()}

            # Combina os dados comuns e específicos
            dados_serie = {**dados_comuns, **dados_especificos}

            lista_de_series.append(pd.Series(dados_serie))

        self.df = pd.DataFrame(lista_de_series)

    def process_table_data(self):
        lista_de_dicionarios = []

        for card in self.data['data']['table']['table_records']['edges']:
            node = card.get('node', {})
            ID = node.get('id')  # Garante que não haverá erro se 'id' não estiver presente
            dados_linha = {'ID': ID}  # Inicializa 'dados_linha' com o ID
            for campo in node['record_fields']:
                titulo = campo.get('name')
                valor = campo.get('report_value')
                dados_linha[titulo] = valor

            lista_de_dicionarios.append(dados_linha)
        self.df = pd.DataFrame(lista_de_dicionarios)

    def process_ipesquisa_data(self):
        lista_de_dicionarios = []

        for dado in self.data:
            # print(dado)
            ID = dado.get('id', {})
            nome = dado.get('nome')  # Garante que não haverá erro se 'id' não estiver presente
            # id_pipefy = nome.split("|")[0].strip()
            try:
                id_pipefy = nome.split("#")[1].split("|")[0].strip()
                if id_pipefy == '':
                    id_pipefy = None
            except:
                id_pipefy = None

            dados_linha = {'id': ID,
                           'id_pipefy': id_pipefy,
                           'nome': nome}
            lista_de_dicionarios.append(dados_linha)
        self.df = pd.DataFrame(lista_de_dicionarios)


    def get_dataframe(self, filepath, field_mapping):
        self.load_json(filepath)
        if self.check_errors():
            self.process_data(field_mapping)
        return self.df


    def get_dataframe2(self, filepath):
        self.load_json(filepath)
        if self.check_errors():
            self.process_table_data()
        return self.df

    def get_dataframe3(self, filepath):
        self.load_json(filepath)
        #if self.check_errors():
        self.process_ipesquisa_data()
        return self.df
