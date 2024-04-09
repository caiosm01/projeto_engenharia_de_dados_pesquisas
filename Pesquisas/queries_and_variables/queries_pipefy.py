query_pipe_projetos = """{allCards(pipeId:303834641) 
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
  }"""


query_database_colaboradores = """{
  table(id: "301587940") {
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
"""