import pyspark
print(pyspark.__version__)
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, col, explode
import time
import os

#Criar a spark session
spark = SparkSession.builder \
    .appName("bookings") \
    .getOrCreate()

#Inicialização de tempo 
start_time = time.time()

#Ler o arquivo de reservas
file = 'file.json'
df = spark.read.json(file, multiLine=True)

# Por ser um dataframe pequeno, estou convertendo o timestamp para um formato de melhor leitura.
# Dependendo da aplicação e sua escalabilidade, manteria o timestamp em long int para melhor armazenamento. 
df = df.withColumn("booking_date", from_unixtime(col("booking_timestamp")))

df = df.withColumn("rental_group_id", col("rental_group.rental_group_id"))
df = df.withColumn("rental_group_name", col("rental_group.rental_group_name"))

#Desagrupando transações
df = df.withColumn("transaction", explode(col("transactions")))

#Extraindo colunas das transações 
df = df.withColumn("transaction_id", col("transaction.transaction_id"))
df = df.withColumn("transaction_method", col("transaction.method"))
df = df.withColumn("transaction_discount", col("transaction.discount"))
df = df.withColumn("transaction_status", col("transaction.status"))
df = df.withColumn("transaction_timestamp", col("transaction.timestamp"))
df = df.withColumn("transaction_value", col("transaction.transaction_value"))
df = df.withColumn("transaction_date", from_unixtime(col("transaction_timestamp")))
df = df.withColumn("gateway_id", col("transaction.gateway.gateway_id"))
df = df.withColumn("gateway_name", col("transaction.gateway.gateway_name"))

#Limpando o dataframe
df = df.drop('transactions','rental_group','transaction')

#Print do nome das colunas e tipagem
df.printSchema()

#Gravando em parquet, particionando pelo 'booking_timestamp'
df.write.partitionBy("booking_timestamp").parquet('/dados')

#Finalizar o tempo e calcular tempo de execução
end_time = time.time()
elapsed_time = end_time - start_time

#Contar quantos arquivos foram salvos
partitioned_dir = '/dados'
num_files = sum([len(files) for r, d, files in os.walk(partitioned_dir)])

#Resultados
print(f"Número de arquivos particionados: {num_files}")
print(f"Tempo de execução: {elapsed_time:.2f} segundos")
print('Fim da aplicação')