from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, udf
from pyspark.sql.types import *

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Definir el esquema del DataFrame
schemaInfo = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("physical_configuration", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("altitude", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("post_code", StringType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("is_charging_station", BooleanType(), True),
    StructField("short_name", IntegerType(), True),
    StructField("nearby_distance", IntegerType(), True),
    StructField("_ride_code_support", BooleanType(), True),
    StructField("rental_uris", StringType(), True)  # Usamos StringType() para manejar valores null
])

# Definir los datos como una lista de diccionarios
data = [
    {
        "station_id": 1,
        "name": "GRAN VIA CORTS CATALANES, 760",
        "physical_configuration": "ELECTRICBIKESTATION",
        "lat": 41.3979779,
        "lon": 2.1801069,
        "altitude": 16,
        "address": "GRAN VIA CORTS CATALANES, 760",
        "post_code": "08013",
        "capacity": 46,
        "is_charging_station": True,
        "short_name": 1,
        "nearby_distance": 1000,
        "_ride_code_support": True,
        "rental_uris": None
    }
]

# Crear el DataFrame
dfInfo = spark.createDataFrame(data, schemaInfo)


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
kafka_password = "VNdAigNg3R"

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
joined_df = data_df.join(dfInfo, on="station_id", how="left")

# Función
def check_bike_status(num_bikes_available, capacity):
    if num_bikes_available is None or capacity is None:
        return 'undefined'
    if capacity == 0:  # To avoid division by zero
        return 'undefined'
    if float(num_bikes_available) <= 0.4 * float(capacity):
        return 'low'
    else:
        return 'sufficient'

# Registrar la función como UDF
check_bike_status_udf = udf(check_bike_status, StringType())

# Crear una nueva columna usando la UDF
joined_df = joined_df.withColumn("check_status", check_bike_status_udf(joined_df["num_bikes_available"], joined_df["capacity"]))

# Escribir los datos procesados a MongoDB
mongo_uri = "mongodb://mongodb:27017/bicing_db.estat_stream"

query = joined_df \
    .writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/stream_estat_estacions/") \
    .option("spark.mongodb.write.connection.uri", mongo_uri) \
    .start()

# Esperar hasta que se detenga el streaming
query.awaitTermination()