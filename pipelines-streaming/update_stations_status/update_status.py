from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, from_unixtime, udf, current_timestamp
from pyspark.sql.types import *

# Configurar el registro
logging.basicConfig(level=logging.INFO)

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Definir el esquema de los datos en streaming Estat Estacions
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
kafka_password = "dIegwBpHw6"

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

# Función
def check_bike_status(num_bikes_available, capacity):
    if num_bikes_available is None or capacity is None:
        return 'undefined'
    if capacity == 0:  # To avoid division by zero
        return 'undefined'
    if float(num_bikes_available) <= 0.4 * float(capacity - (25 * capacity / 100)):
        return 'low'
    else:
        return 'sufficient'

# Registrar la función como UDF
check_bike_status_udf = udf(check_bike_status, StringType())

# Unir con el DataFrame que contiene el estado de las estaciones y los clusters
joined_df = data_df \
    .join(dfInfo_with_clusters, on="station_id", how="left") \
    .withColumn("check_status", check_bike_status_udf(data_df["num_bikes_available"], dfInfo_with_clusters["capacity"]))

to_neo4j = joined_df \
    .select("station_id", "capacity", "cluster") \
    .withColumn("lastUpdate", current_timestamp())

# Write processed data to Neo4j
query = to_neo4j \
    .writeStream \
    .format("org.neo4j.spark.DataSource") \
    .option("url", "neo4j://neo4j-neo4j:7687") \
    .option("checkpointLocation", "/tmp/stream_estat_estacions/") \
    .option("labels", "Station") \
    .option("node.keys", "station_id") \
    .option("save.mode", "Overwrite") \
    .start() \
    .awaitTermination()
