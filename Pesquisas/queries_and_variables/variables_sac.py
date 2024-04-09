import os
from dotenv import load_dotenv

#load_dotenv(override=True)
#caminho_pasta = os.getenv("caminho_pasta")

dict_mappings = {
                'Aeroporto': {
                                "Sbbe":1,
                                "Sbbr":2,
                                "Sbcf":3,
                                "Sbct":4,
                                "Sbcy":5,
                                "Sbeg":6,
                                "Sbfl":7,
                                "Sbfz":8,
                                "Sbgl":9,
                                "Sbgo":10,
                                "Sbgr":11,
                                "Sbkp":12,
                                "Sbmo":13,
                                "Sbpa":14,
                                "Sbrf":15,
                                "Sbrj":16,
                                "Sbsg":17,
                                "Sbsp":18,
                                "Sbsv":19,
                                "Sbvt":20
                },
                'Processo': {
                                "Cartão de embarque":1,
                                "Check in":2,
                                "Controle aduaneiro":3,
                                "Emigração":4,
                                "Imigração":5,
                                "Inspeção de segurança":6,
                                "Restituição de bagagem":7,
                                "Embarque":8,
                                "Desembarque":9
                },
                'Estrato': {
                                "Doméstico":1,
                                "Internacional":2
                },
                'Companhia': {
                                "Azul":1,
                                "Gol":2,
                                "Latam":3,
                                "N/A":4
                },
                'Procedimento':{
                                "Pesquisa":1,
                                "Medição":2
                }
}


# dict_Aero = {'SBBE':1,
#                "SBBR":2,
#                "SBCF":3,
#                "SBCT":4,
#                "SBCY":5,
#                "SBEG":6,
#                "SBFL":7,
#                "SBFZ":8,
#                "SBGL":9,
#                "SBGO":10,
#                "SBGR":11,
#                "SBKP":12,
#                "SBMO":13,
#                "SBPA":14,
#                "SBRF":15,
#                "SBRJ":16,
#                "SBSG":17,
#                "SBSP":18,
#                "SBSV":19,
#                "SBVT":20}
#
#
# dict_Estrato = {'Doméstico':1,
#                "Internacional":2}
#
#
# dict_processo = {'Cartão de embarque':1,
#                "Check in":2,
#                "Controle aduaneiro":3,
#                "Emigração":4,
#                "Imigração":5,
#                "Inspeção de segurança":6,
#                "Restituição de bagagem":7,
#                "Embarque":8,
#                "Desembarque":9}


avaliacoes = {
        'Muito bom': '5', '5- Muito bom': '5', '5 - Muito bom': '5', 'Very good': '5', '5- Very good': '5', '5 - Very good': '5',
        'Bom': '4', '4- Bom': '4', '4 - Bom': '4', 'Good': '4', '4- Good': '4', '4 - Good': '4',
        'Regular': '3', '3- Regular': '3', '3 - Regular': '3',
        'Ruim': '2', '2- Ruim': '2', '2 - Ruim': '2', 'Bad': '2', '2- Bad': '2', '2 - Bad': '2',
        'Muito ruim': '1', '1- Muito ruim': '1', '1 - Muito ruim': '1', 'Very bad': '1', '1- Very bad': '1', '1 - Very bad': '1',
        'NS/NR/NA': 'NS/NR'
    }

satisfacao_columns = """
                 id_aeroportos,
                 id_estrato,
                 id_processo,
                 L5,
                 Concorda_em_participar,
                 L6,
                 L9,
                 L9_a,
                 L10,
                 L11,
                 L12,
                 L13,
                 L13_a,
                 C3,
                 L14,
                 L14_a,
                 D1,
                 D2,
                 D2_a,
                 D3,
                 D3_a,
                 D4,
                 D4_a,
                 D5,
                 A1,
                 A2,
                 A3,
                 C1,
                 C2,
                 C2_a,
                 C2_b,
                 C2_c,
                 C2_d,
                 C2_e,
                 C2_f,
                 C4,
                 S1,
                 S1_a,
                 S1_b,
                 S1_c,
                 M1,
                 M2,
                 M2_a,
                 M2_b,
                 M2_c,
                 M3,
                 M4,
                 M4_a,
                 M4_b,
                 M4_c,
                 M5,
                 M5_a,
                 M5_b,
                 M5_c,
                 M5_d,
                 O1,
                 O1_a,
                 O1_b,
                 O1_c,
                 O1_d,
                 O2,
                 O2_a,
                 O2_b,
                 O2_c,
                 B2,
                 B3,
                 B3_a,
                 B3_b,
                 B3_c,
                 B4,
                 R1,
                 R1_a,
                 R1_b,
                 R1_c,
                 R2,
                 R2_a,
                 R2_b,
                 R2_c,
                 R3,
                 R4,
                 R5,
                 R6,
                 R6_a,
                 R6_b,
                 R7,
                 R8,
                 R8_a,
                 R8_b,
                 R8_c,
                 R9,
                 G1,
                 G2,
                 P1,
                 P2,
                 P3,
                 P4,
                 P5,
                 P5_a,
                 P6,
                 P7,
                 P8,
                 P8_a,
                 P9,
                 P10,
                 P11,
                 P13,
                 P14,
                 P12,
                 P12_a,
                 N1,
                 N1_a,
                 N2,
                 N3,
                 Pesquisador0,
                 `Nro. Identificação`"""


medicao_columns = """
            `id_aeroportos`,
            `id_processo`,
            `id_estrato`,
            `4- Companhia aérea?`,
            `4.1 - OUTROS:`,
            `5- Tipo de fila?`,
            `9- Número do Vôo:`,
            `10- Quantidade de Esteiras?`,
            `PAX`,
            `ENTRADA NA FILA`,
            `SAIDA DA FILA`,
            `TEMPO NA FILA`,
            `ABANDONOU FILA`,
            `PAX ATRAS`,
            `PAX FRENTE`,
            `DESEMBARQUE DO PRIMEIRO PASSAGEIRO`,
            `NUMERO DE GUICHES`,
            `LOGS DE TROCA DE NUMERO DE GUICHES`,
            `PASSAGEIROS?  (Perguntar ao agente da CIA)`,
            `8- Troca de portão?`,
            `11- Calço da aeronave?`,
            `Pesquisador.1`,
            `Nro. Identificação`
            """

pesquisas_columns = """idProjeto, idQuestionario, idFuncionario, idPesquisa, Data_Inicio, Data_Fim, Latitude, Longitude"""

projetos_columns = """Projeto, idProjeto, fase_atual, inicio_projeto, Data_pre_teste_campo, Data_pre_teste_cati, inicio_campo, inicio_cati, fim_campo, fim_cati"""

funcionarios_columns = """idFuncionario, Status, Nome, email_pessoal, email_institucional, Telefone, cargo, data_nasc, estado_civil, escolaridade, sexo"""

escala_columns = """Pesquisador, id_aeroportos, Data"""

meta_columns = """`Pesquisador`,`id_aeroportos`,`id_processo`, `id_estrato`,`id_companhia`,`id_tipo`,`META`,`Mês`,`Ano`"""

ipesquisa_columns = """id_ipesquisa, id_pipefy, nome"""


def get_columns(table_name):
    columns_mapping = {
        'banco_sac2.satisfacao': satisfacao_columns,
        'banco_sac2.medicao': medicao_columns,
        'mydb.pesquisas': pesquisas_columns,
        'banco_sac2.escala_pesquisadores':escala_columns,
        'banco_sac2.meta_pesquisadores': meta_columns,
        'mydb.i_pesquisa': ipesquisa_columns
    }

    return columns_mapping.get(table_name, [])


# caminhos = {
#             # 'Outros':[
#             #             {
#             #                 'geral':
#             #                     [
#             #                         {
#             #                             'id_ipesquisa': 4388,
#             #                             'id_pipe': 858086642
#     #                             }
#             #                     ]
#             #             }
#             #         ],
#             'SAC':
#                 [
#                     {
#                     'satisfação':
#                         [
#                             {
#                                 'id_ipesquisa': 4607,
#                                 'id_pipe': 884736479
#                             }
#                         ]
#                     },
#
#                     {'medição':
#                          [
#                              {
#                                 'id_ipesquisa': 4608,
#                                 'id_pipe': 884736479
#                              }
#
#                         ]
#                     },
#
#                     {'satisfação em inglês':
#                          [
#                              {
#                                 'id_ipesquisa': 4609,
#                                 'id_pipe': 884736479
#                              }
#
#                         ]
#                     }
#                     ]
#             }
#
