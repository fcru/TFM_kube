from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, from_unixtime, current_timestamp, when, lit
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
    StructField("last_reported", LongType(), True),
    StructField("is_charging_station", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("is_installed", IntegerType(), True),
    StructField("is_renting", IntegerType(), True),
    StructField("is_returning", IntegerType(), True),
    StructField("traffic", StringType(), True)
])

# Read data from Neo4j
dfInfo_with_clusters = spark.read \
    .format("org.neo4j.spark.DataSource") \
    .option("url", "neo4j://neo4j-neo4j:7687") \
    .option("labels", "Station") \
    .load() \
    .alias("info")

# Kafka Configuration and reading the stream
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "estat_estacions"
kafka_username = "user1"
kafka_password = "tiSmu50tsg"

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
    .select("data.*") \
    .alias("data")

# Convert `last_reported` from Unix timestamp to proper timestamp
data_df = data_df.withColumn("last_reported", from_unixtime(col("last_reported")).cast("timestamp"))

# Renombrar la columna num_bikes_available en dfInfo_with_clusters
#dfInfo_with_clusters = dfInfo_with_clusters.withColumnRenamed("num_bikes_available", "info_num_bikes_available")

# Select the desired columns
data_df = data_df.select("station_id", "num_bikes_available", "last_reported")
dfInfo_with_clusters = dfInfo_with_clusters.select("station_id", "capacity", "truck", "lat", "lon")

# Join data from Kafka with data from Neo4j based on "station_id"
def join_data(data_df: DataFrame, dfInfo_with_clusters: DataFrame) -> DataFrame:
    joined_df = data_df.join(
        dfInfo_with_clusters,
        on="station_id",
        how="left"
    )
    return joined_df

# Assign a status and calculate the bikes to be replaced or removed
def assign_status(joined_df: DataFrame) -> DataFrame:
    status_df = joined_df.withColumn(
        "check_status",
        when(col("num_bikes_available").isNull() | col("capacity").isNull(), "undefined") \
        .when(col("capacity") == 0, "undefined") \
        .when(col("num_bikes_available") <= 0.4 * (col("capacity") - (0.25 * col("capacity"))), "low") \
        .when(col("capacity") == col("num_bikes_available"), "full") \
        .otherwise("sufficient")
    ).withColumn(
        "bikes_to_refill",
        when(col("check_status") == "low", (col("capacity") - (0.25 * col("capacity")) - col("num_bikes_available")).cast(IntegerType())).otherwise(None)
    ).withColumn(
        "bikes_to_remove",
        when(col("check_status") == "full", (0.25 * col("capacity")).cast(IntegerType())).otherwise(None)
    )
    return status_df

joined_df = join_data(data_df, dfInfo_with_clusters)
status_df = assign_status(joined_df)

# Add the `lastUpdate` column
to_neo4j = status_df \
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

