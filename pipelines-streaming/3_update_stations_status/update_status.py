from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, udf, current_timestamp, sum as spark_sum, when, lit
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define the schema of the streaming data from Kafka
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

# Read data from Neo4j
dfInfo_with_clusters = spark.read \
    .format("org.neo4j.spark.DataSource") \
    .option("url", "neo4j://neo4j-neo4j:7687") \
    .option("labels", "Station") \
    .load()

# Kafka Configuration and reading the stream
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "estat_estacions"
kafka_username = "user1"
kafka_password = "4upSLqDLTX"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

# Convert the Kafka message to columns
data_df = df.selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert `last_reported` from Unix timestamp to proper timestamp
data_df = data_df.withColumn("last_reported", from_unixtime(col("last_reported")).cast("timestamp"))

# Function to check the status of the bike station
def check_bike_status(num_bikes_available, capacity):
    if num_bikes_available is None or capacity is None:
        return 'undefined'
    if capacity == 0:
        return 'undefined'
    if float(num_bikes_available) <= 0.4 * float(capacity - (25 * capacity / 100)):
        return 'low'
    if capacity == num_bikes_available:
        return 'full'
    else:
        return 'sufficient'

# Register the function as UDF
check_bike_status_udf = udf(check_bike_status, StringType())

# Join data from Kafka with data from Neo4j based on "station_id"
joined_df = data_df \
    .join(dfInfo_with_clusters, on="station_id", how="left") \
    .withColumn("check_status", check_bike_status_udf(data_df["num_bikes_available"], dfInfo_with_clusters["capacity"]))

# Prepare data to write back to Neo4j, adding the `lastUpdate` column
to_neo4j = joined_df \
    .select("station_id", "capacity", "truck", "check_status", "num_bikes_available", "lat", "lon") \
    .withColumn("lastUpdate", current_timestamp())

# Write the updated data back to Neo4j
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
