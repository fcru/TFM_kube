from pyspark.sql import SparkSession

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("StructuredStreamingKafkaExample") \
        .master("local[*]") \
        .getOrCreate()

    # Kafka configuration for Structured Streaming
    kafka_conf = {
        'kafka.bootstrap.servers': 'kafka:9092',
        'subscribe': 'my-topic',
        'kafka.security.protocol': 'SASL_PLAINTEXT',  # Or 'SASL_SSL' if using SSL
        'kafka.sasl.mechanism': 'PLAIN',
        'startingOffsets': 'latest'
    }

    # Read stream from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()


    # Process the messages
    processed_stream = kafka_stream \
        .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")

    # Save data in Mongodb
    mongoQuery = processed_stream.writeStream \
           .format("mongodb") \
           .option("spark.mongodb.connection.uri", "mongodb://mongodb:27017") \
           .option("database", "testdb") \
           .option("collection", "uuids") \
           .option("checkpointLocation", "/tmp/pyspark/") \
           .outputMode('append') \
           .start()


    # Wait until the application is terminated
    mongoQuery.awaitTermination()

if __name__ == "__main__":
    main()