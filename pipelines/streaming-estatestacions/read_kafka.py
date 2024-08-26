from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import *

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Definir el esquema de los datos esperados
schema = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_bikes_available_types", StructType([
        StructField("mechanical", IntegerType(), True),
        StructField("ebike", IntegerType(), True)
    ]), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("last_reported", LongType(), True),  # Usamos LongType porque es un timestamp en segundos
    StructField("is_charging_station", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("is_installed", IntegerType(), True),
    StructField("is_renting", IntegerType(), True),
    StructField("is_returning", IntegerType(), True),
    StructField("traffic", StringType(), True)  # Puede ser null
])

# Configuración de los parámetros para conectarse a Kafka con SASL/PLAIN
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "estat_estacions"
kafka_username = "user1"
kafka_password = "YDzYu3qDJN"

# Leer el stream de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

# Convertir el JSON a columnas usando el esquema definido
data_df = df.selectExpr("CAST(value AS STRING) as value").select(from_json(col("value"), schema).alias("data"))

# Convertir el campo last_reported a un timestamp
data_df = data_df.select("data.*") # Expandimos todas las columnas para asegurarnos que accedemos correctamente
data_df = data_df.withColumn("last_reported", from_unixtime(col("last_reported")).cast("timestamp"))

# Mostrar los datos en la consola
query = data_df \
    .writeStream \
    .option("truncate", "false") \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar hasta que se detenga el streaming
query.awaitTermination()
