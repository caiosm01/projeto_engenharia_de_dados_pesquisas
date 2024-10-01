<h1>Engenharia de dados</h1>

<p>Com o objetivo de centralizar dados provenientes de diversas plataformas em um único repositório e relacioná-los para extrair insights eficazes, foi desenvolvido um Data Warehouse (DW) com um conjunto específico de regras de negócio, visando atender às necessidades da empresa.</p>
<p>Para alimentar esse DW, foi criado um pipeline de ETL (Extract, Transform, Load) que busca dados de diversas fontes e formatos, utilizando planilhas e APIs. Esse processo permite relacionar dados que anteriormente não eram analisados em conjunto, pois não estavam conectados em um repositório central.</p>
<img src="https://github.com/caiosm01/projeto_engenharia_de_dados_agora/blob/main/fluxograma_ETL.png"  height="450px" width="1720px">

<h2>Estrutura do DW</h2>
<p>O DW é composto por 3 databases, com tabelas relacionadas entre si.</p>
<ol>
    <li><h3>banco mydb:</h3></li>
        <p>O principal é o mydb, onde ficam as tabelas com informações dos funcionários, dos projetos e das pesquisas.</p>
        <img src="https://github.com/caiosm01/projeto_engenharia_de_dados_agora/blob/main/principal.png"  height="600px" width="700px">
        <h4>Tabela projetos</h4>
        <table>
          <thead>
            <tr>
              <th>Column Name</th>
              <th>Datatype</th>
              <th>PK</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Projeto</td>
              <td>TEXT</td>
              <td></td>
            </tr>
            <tr>
              <td>idProjeto</td>
              <td>BIGINT</td>
              <td>✅</td>
            </tr>
            <tr>
              <td>fase_atual</td>
              <td>INT</td>
              <td></td>
            </tr>
            <tr>
              <td>inicio_projeto</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>Data_pre_teste_campo</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>Data_pre_teste_cati</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>inicio_campo</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>inicio_cati</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>fim_campo</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>fim_cati</td>
              <td>DATE</td>
              <td></td>
            </tr>
            <tr>
              <td>Quantidade_pergunta</td>
              <td>INT</td>
              <td></td>
            </tr>
            <tr>
              <td>tipo_projeto</td>
              <td>VARCHAR(100)</td>
              <td></td>
            </tr>
            <tr>
              <td>tipo_pagamento</td>
              <td>VARCHAR(45)</td>
              <td></td>
            </tr>
            <tr>
              <td>quantidade_pesquisas</td>
              <td>INT</td>
              <td></td>
            </tr>
            <tr>
              <td>prioridade</td>
              <td>INT</td>
              <td></td>
            </tr>
          </tbody>
        </table>
</ol>

<h2>Requisitos</h2>

<p><strong>Arquivo <code>.env</code>:</strong> O programa requer um arquivo <code>.env</code> na raiz do projeto que contém várias variáveis de ambiente usadas pelo script. Estas variáveis incluem informações de autenticação do banco de dados, caminhos de arquivo e coordenadas de tela para simulação de cliques.</p>

<h3>Exemplo de conteúdo do arquivo <code>.env</code>:</h3>

<div style="background-color: #2C2C2C; color: #A9B7C6; padding: 10px; border-radius: 5px; font-family: monospace; white-space: pre-wrap;">
  
    usuario = 'Admin'                                                          ##Nome do usuário da máquina 
    profile_ = 'Profile 1'                                                     ##Endereço do navegagor onde o drive que deseja acessar já está logado. Acesse `Chrome://version` para descobrir
    host = 'localhost'                                                         ##Nome host ou IP do banco de dados
    data_base = 'banco'                                                        ##Nome do banco de dados
    User= 'root'                                                               ##Nome do usuário de acesso ao banco de dados
    Password= 'senha'                                                          ##Senha de acesso do usuário
    Query_banco = 'select * from banco.tabela;'                                ##Query de seleção da tabela desejada
    Query_banco_id = 'SELECT coluna FROM banco.tabela2 where coluna2='         ##Parte da query para fornecer a sigla do aeroporto, de acordo com o seu id que será processada no código

    link_x = 1116                                                              ##Coordenada x do botão 'Copiar link'
    link_y = 761                                                               ##Coordenada y do botão 'Copiar link'

    concluido_x = 1459                                                         ##Coordenada x do botão 'Concluído'
    concluido_y = 761                                                          ##Coordenada y do botão 'Concluído'

    planilha = 'C:/Users/Admin/Documents/planilha_de_registros.xlsx'           ##Planilha na qual deseja registrar os links copiados
    sheet_name= 'Planilha1'                                                    ##Nome da pasta de trabalho
    origem_audio = 'C:/Users/Admin/Documents/Áudios1/'                         ##Pasta de origem do arquivo de áudio
    destino_audio = 'C:/Users/Admin/Documents/Áudios2/'                        ##Pasta de destino do arquivo de áudio, onde será feita uma cópiaa
</div>

<p><em>Nota:</em> Preencha os valores de cada variável conforme suas configurações.</p>

<h2>Como Usar</h2>

<ol>
    <li>Clone o repositório ou faça o download do código fonte.</li>
    <li>Certifique-se de ter todas as bibliotecas necessárias instaladas.</li>
    <li>Crie e configure o arquivo <code>.env</code> conforme mencionado acima.</li>
    <li>Execute o script principal para iniciar o monitoramento e a manipulação dos arquivos.</li>
</ol>
