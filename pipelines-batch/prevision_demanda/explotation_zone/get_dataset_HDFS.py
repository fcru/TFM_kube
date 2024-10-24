import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from analisis_functions import categorize_hour, get_season, save_dataset

def read_estado_data():
    spark = SparkSession.builder \
        .appName("Read data Estado") \
        .getOrCreate()
    
    spark.sparkContext.addPyFile("analisis-demanda/analisis_functions.py")
    
    # Lectura recursiva del directorio "estat-final" que es donde están almacenados todos los ficheros parquet,
    # segmentados por año y mes, con los datos del estado de las estaciones de Bicing
    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat-final"
    df = spark.read.option("recursiveFileLookup", "true").parquet(estat_path)
    df.printSchema()

    categorize_hour_udf = F.udf(categorize_hour, StringType())
    get_season_udf = F.udf(get_season, StringType())
    df = df.withColumn("status_exploded", F.explode(F.col("status"))) 

    df = df.selectExpr(
        "*", 
        "status_exploded.hora_dia as hora_dia",
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
        "status_exploded.weather.viento as viento",
        "status_exploded.last_reported_local as last_reported_local",
    )

    df = df.withColumn("year", F.year(F.col("date_reported"))) \
        .withColumn("month", F.month(F.col("date_reported"))) \
        .withColumn("day", F.dayofmonth(F.col("date_reported"))) \
        .withColumn("hora_info", categorize_hour_udf(F.col("hora_dia"))) \
        .withColumn("season", get_season_udf(F.col("date_reported")))

    df = df.drop("status", "status_exploded", "date_reported")
    df.printSchema()

    df = df.dropDuplicates()

    local_output_path = "/opt/data/estado_bicing.csv"
    save_dataset(df, local_output_path)

    spark.stop()

def read_stations_data():
    spark = SparkSession.builder \
        .appName("Read data Estado") \
        .getOrCreate()
    
    # Lectura recursiva del directorio "informacio" que es donde están almacenados todos los ficheros parquet,
    # segmentados por año y mes, con los datos de información de las estaciones de Bicing
    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio"
    df = spark.read.option("recursiveFileLookup", "true").parquet(estat_path)
    df.printSchema()

    # Nos quedamos con la última información asociada a la estación, si durante el tiempo ha cambiado
    windowSpec = Window.partitionBy("station_id").orderBy(F.col("date").desc())
    df_with_rank = df.withColumn("rank", F.row_number().over(windowSpec))

    df_stations = df_with_rank.filter(F.col("rank") == 1) \
        .select("station_id", "name", "address", "post_code", "lon", "lat", "altitude") # No podemos guardar en csv la columna con las coordenadas geoJSON que tenemos

    save_dataset(df_stations,"/opt/data/info_estaciones.csv")
    
    spark.stop()

def show_options():
    print("Elije la opción que quieres ejecutar:")
    print("\t 0 - Salir")
    print("\t 1 - Descargar dataset (CSV) con el estado de las estaciones del Bicing")
    print("\t 2 - Descargar dataset (CSV) con la información de las estaciones")


if __name__ == "__main__":
    show_options()
    op = int(input())

    while op != 0:
        if op == 1:
            read_estado_data()
        elif op == 2:
            read_stations_data()
        else:
            print ("Exiting ...")
            sys.exit()

        show_options()
        op = int(input())