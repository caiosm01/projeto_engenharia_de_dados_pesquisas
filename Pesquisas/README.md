
<h1>Pesquisas</h1>

<p>O projeto <strong>Pesquisas</strong> foi desenvolvido para automatizar o processo de download de arquivos csv das pesquisas feitas no sistema do iPesquisa e adicionar seus dados no banco de dados da empresa, de forma automática e periódica.</p>
<p>O processo é bem simples, sendo o primeiro passo o acesso ao site do iPesquisa através da biblioteca Selenium, com o python. Após o acesso, o código deve acessar os códigos que foram especificados, um de cada vez, dentro da variável "caminho", que é um dicionário. Através desse endereço (o id da pesquisa dentro do sistema) o código irá baixar os arquivos nas pastas que foram configuradas, diretamente no novegador do Google Chrome (o local do download não é especificado através do algoritmo)</p>
<p>Com os arquivos baixados entra em ação a segunda etapa, que consiste na leitura dos arquivos, o tratamento dos dados e a inserção desses dados no banco de dados.</p>
<p>Dentro da pasta de funções temos as seguintes funcionalidades</p>
<h2>Funcionalidades</h2>

<ol>
    <li><strong>Conexão com o banco de dados:</strong> Arquivo que realiza qualquer tipo de interação com o banco, como iniciar e encerrar a conexão, executar queries, etc.</li>
    <li><strong>Manipulação de Dataframes:</strong> Arquivo com várias funções que realizam manipulações de dados que são em comum para a mairia dos dataframes que são gerados</li>
    <li><strong>Conexão com o Pipefy, via API:</strong> Algorítimo que se conecta com o pipefy, busca os dados solicitados. A requisição é feita sempre levando em consideração o limite de chamadas mensais. Se o limite for atingido, a requisição não será feita.</li>
    <li><strong>Transformação de arquivos JSON em dataframes:</strong> Os dados que são lidos através da chamada de API, são registrados em arquivos JSON. Com isso, para facilitar a manipulação desses dados, existe uma série de funções para transformar esse arquivo em um dataframe.</li>
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

    planilha = 'C:/Users/Admin/Documents/planilha_de_registros.xlsx'           ##Planilha na qual deseja registrar os links copiados
    sheet_name= 'Planilha1'                                                    ##Nome da pasta de trabalho
    origem_audio = 'C:/Users/Admin/Documents/Áudios1/'                         ##Pasta de origem do arquivo de áudio
    destino_audio = 'C:/Users/Admin/Documents/Áudios2/'                        ##Pasta de destino do arquivo de áudio, onde será feita uma cópiaa
</div>

<p><em>Nota:</em> Preencha os valores de cada variável conforme suas configurações.</p>

<h2>Como Usar</h2>

<ol>
    <li>Clone o repositório ou faça o download do código fonte.</li>
    <li>Certifique-se de ter todas as bibliotecas necessárias instaladas (pip install -r requirements.txt).</li>
    <li>Crie e configure o arquivo <code>.env</code> conforme mencionado acima.</li>
    <li>Execute o script principal (main.py) para iniciar o monitoramento e a manipulação dos arquivos.</li>
</ol>
