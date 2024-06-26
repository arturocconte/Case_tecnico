# Case_tecnico
Case técnico utilizando PySpark, Docker e Airflow.

O teste consistia em ler um arquivo JSON em PySpark, identificar as colunas e tipagens do arquivo e particionar os dados pela coluna de Timestamp.

Foi feita uma imagem Docker com esse arquivo e upado para o DockerHub.

Com essa imagem, foi construída uma DAG em Airflow que rodaria a imagem todos os dias da semana, das 11h-21h, de hora em hora, exceto nas quartas-feiras.
