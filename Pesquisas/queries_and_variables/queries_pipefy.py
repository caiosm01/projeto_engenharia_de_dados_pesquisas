from dotenv import load_dotenv
import os

################################################### VARIÁVEIS PRIVADAS ###############################################
load_dotenv(override=True)
id_projeto = os.getenv("id_projeto")
id_funcionarios = os.getenv("id_funcionarios")

query_pipe_projetos = """{allCards(pipeId: %d) 
  {edges
    { 
      node{
        current_phase{
          name}
        id 
        title 
        fields
        {name 
          value
        }
      }
    }
    }
  }""".format(id_projeto)


query_database_colaboradores = """{
  table(id: "%d") {
    table_records(last: 30) {
      edges {
        node {
          id
          record_fields {
            name
            report_value
          }
        }
      }
    }
  }
}
""".format(id_funcionarios)