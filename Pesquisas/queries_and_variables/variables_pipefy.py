import os
from dotenv import load_dotenv

load_dotenv(override=True)

usuario_maquina = os.getenv("usuario_maquina")

caminho_json_projetos = f"C:\\Users\\{usuario_maquina}\\PycharmProjects\\Pesquisas\\arquivos\\projetos.json"
caminho_json_funcionarios = f"C:\\Users\\{usuario_maquina}\\PycharmProjects\\Pesquisas\\arquivos\\funcionarios.json"

fases_pipe_projetos = {'Definição do Projeto':1,
               "Briefing":2,
               "Cronograma e Solicitações ao RH":3,
               "Plano Amostral":4,
               "Questionários e Roteiros":5,
               "Pré-teste":6,
               "Recursos Humanos":7,
               "Campo":8,
               "Processamento de Dados":9,
               "Relatório e Apresentação":10,
               "Em Finalização":11,
               "Finalizado":12,
               "Verificado":13,
               "Cancelados":14}


field_mapping_projetos = {
    "Data de início do Projeto": "inicio_projeto",
    'Data do pré-teste da Pesquisa F2F': "Data_pre_teste_campo",
    "Data do pré-teste da Pesquisa CATI": "Data_pre_teste_cati",
    "Início | Etapa F2F":"inicio_campo",
    "Início | Etapa CATI": "inicio_cati",
    "Fim | Etapa F2F": "fim_campo",
    "Fim | Etapa CATI":"fim_cati"
}

field_mapping_coordenador_projetos = {
    "Coordenador do Projeto": "Nome"
}