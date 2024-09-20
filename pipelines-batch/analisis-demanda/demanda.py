from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def readEstadoData():
    spark = SparkSession.builder \
        .appName("Read data Estado") \
        .getOrCreate()
    #sc = spark.sparkContext
    
    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat-final"
    df = spark.read.option("recursiveFileLookup", "true").parquet(estat_path)
    df.printSchema()

    df = df.withColumn("status_exploded", F.explode(F.col("status")))
    df = df.withColumn("station_id", F.col("station_id").cast(IntegerType()))

    df = df.selectExpr(
        "*", 
        "status_exploded.hora_dia as hora_dia",
        "status_exploded.last_reported_local as last_reported_local",
        "status_exploded.num_bikes_available as num_bikes_available",
        "status_exploded.num_docks_available as num_docks_available",
        "status_exploded.num_bikes_error as num_bikes_error",
        "status_exploded.total_bikes_taken as total_bikes_taken",
        "status_exploded.total_bikes_returned as total_bikes_returned",
        "status_exploded.status as status_station",
        "status_exploded.festivo as festivo",
        "status_exploded.weekday as weekday",
        "status_exploded.laborable as laborable",
        "status_exploded.weather.temperatura as temperatura",
        "status_exploded.weather.precipitacion as precipitacion",
        "status_exploded.weather.humedad as humedad",
        "status_exploded.weather.viento as viento"
    )

    df = df.drop("status", "status_exploded")
    df.printSchema()

    local_output_path = "/home/lfiguerola/TFM/estado_bicing.csv"
    df.coalesce(1).write.mode("overwrite").csv(local_output_path, header=True)

    spark.stop()

if __name__ == "__main__":
    readEstadoData()