# El objetivo del script es guardar en el directorio trusted_zone de HDFS, dentro del directorio año/mes, un fichero por cada día con la información del estado de las estaciones 
# + una columna para cada variable metereológica con el valor correspondiente en el período de tiempo correspondiente + una columna con la capacidad de la estación para ese día
# + una columna con el día de la semana al que pertenece la fecha en cuestión + una columna que indique si ese día era festivo o no + una columna que indica si era laborable 
# (no festivo y no sabado y no domingo)  + un campo calculado que nos indique el número de bicis realmente disponibles y otro el número de bicis estropeadas
# En el estado de las estaciones hay ficheros que no corresponden con el año/mes que estamos tratando. Lo primero que haremos es unificar los datos para que los ficheros que 
# están fuera del mes correspondiente se agreguen al fichero del primer día del mes con hora 00:00

import sys
import os
import time
import gc
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

def showFormattedData():
    spark = SparkSession.builder \
            .appName("Show Formatted data") \
            .getOrCreate()
        
    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/estat/2024/07/2024_07_31.parquet"
    df_estat = spark.read.parquet(estat_path, header=True, inferSchema=True)
    df_estat.persist()
    print("ESTAT")
    df_estat.printSchema()
    df_estat.show()
    df_estat.unpersist()

    informacio_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/informacio/2024/07/2024_07_31.parquet"
    df_informacio = spark.read.parquet(informacio_path, header=True, inferSchema=True)
    df_informacio.persist()
    print("INFORMACIO")
    df_informacio.printSchema()
    df_informacio.show()
    df_informacio.unpersist()

    festivos_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/festivos/holidays.parquet"
    df_festivos = spark.read.parquet(festivos_path, header=True, inferSchema=True)
    df_festivos.persist()
    print("FESTIUS")
    df_festivos.printSchema()
    df_festivos.show()
    df_festivos.unpersist()

    temperatura_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/meteocat/temperatura/2024/07/temperatura_2024_07_31.parquet"
    df_temperatura = spark.read.parquet(temperatura_path, header=True, inferSchema=True)
    df_temperatura.persist()
    print("TEMPERATURA")
    df_temperatura.printSchema()
    df_temperatura.show()
    df_temperatura.unpersist()

    humedad_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/meteocat/humedad/2024/07/humedad_2024_07_31.parquet"
    df_humedad = spark.read.parquet(humedad_path, header=True, inferSchema=True)
    df_humedad.persist()
    print("HUMEDAD")
    df_humedad.printSchema()
    df_humedad.show()
    df_humedad.unpersist()

    precipitacion_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/meteocat/precipitacion/2024/07/precipitacion_2024_07_31.parquet"
    df_precipitacion = spark.read.parquet(precipitacion_path, header=True, inferSchema=True)
    df_precipitacion.persist()
    print("PRECIPITACION")
    df_precipitacion.printSchema()
    df_precipitacion.show()
    df_precipitacion.unpersist()

    viento_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/meteocat/viento/2024/07/viento_2024_07_31.parquet"
    df_viento = spark.read.parquet(viento_path, header=True, inferSchema=True)
    df_viento.persist()
    print("VIENTO")
    df_viento.printSchema()
    df_viento.show()
    df_viento.unpersist()

    spark.stop()
    spark._jvm.System.gc()

def merge_informacio(spark, df, str_date, year, mes):
    print("Mergin capacity")
    info_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/informacio/{year}/{str(mes).zfill(2)}"
    info_file = f"{info_path}/{str_date}.parquet"

    if check_file_existence_hdfs(spark, info_file):
        df_informacio = spark.read.parquet(info_file, header=True, inferSchema=True)
        df_informacio.persist()

        #Estructuramos el dataframe de información para dejarlo en HDFS con la forma deseada antes de traspasarlo en una colección de MongoDB
        df_informacio = df_informacio \
            .withColumn("post_code", F.concat(F.lit('0'), F.col("post_code"))) \
            .withColumn("lon", F.col("lon").cast(DoubleType())) \
            .withColumn("lat", F.col("lat").cast(DoubleType())) \
            .withColumn("altitude", F.col("altitude").cast(DoubleType())) \
            .withColumn("geoJSON_coord", F.array(F.col("lon"), F.col("lat"), F.col("altitude"))) \
            .select("station_id", "date", "name", "address", "post_code", "geoJSON_coord", F.col("max_capacity").alias("capacity"))

        #Merge dataframes para informar de la capacidad en de la estación en el estado
        df_merged = df.join(df_informacio.select("station_id", "capacity"), on="station_id", how="left")

        #Almacenamos los datos de información en HDFS
        # informacio_path_dest = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio/{year}/{str(mes).zfill(2)}"
        # informacio_file_dest = f"{informacio_path_dest}/{file_date_str}.parquet"
        
        # df_informacio.write.mode("overwrite").parquet(informacio_file_dest)
        # print(f"File saved in HDFS: {informacio_file_dest}")

        #Liberamos memoria
        df_informacio.unpersist()
        del df_informacio
        spark._jvm.System.gc()
        gc.collect()
    else:
        df_merged = df.withColumn("capacity", F.lit(0))
    
    return df_merged

def merge_festivos(spark, df):
    print("Marging festivos")
    holiday_file = "hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/festivos/holidays.parquet"
    df_festivos = spark.read.parquet(holiday_file, header=True, inferSchema=True)
    df_festivos.persist()

    df = df.join(F.broadcast(df_festivos), df["date_reported"] == df_festivos["data"], how="left")
    
    df = df \
        .withColumn("hora_dia", F.substring(F.col("hour_reported"), 1,2).cast(IntegerType())) \
        .withColumn("festivo", F.when(F.col("data").isNotNull(), F.lit(1)).otherwise(F.lit(0))) \
        .withColumn("weekday", ((F.dayofweek('date_reported')+5)%7)+1) \
        .withColumn("laborable",
                    F.when((F.col("festivo") == 0) & (F.col("weekday").between(1, 5)), F.lit(1)).otherwise(F.lit(0))
                    )
    
    df_festivos.unpersist()
    del df_festivos
    spark._jvm.System.gc()
    gc.collect()

    return df

def add_calc_variables(df, str_date):
    print("Calculando resto de variables")
    #Creación de nuevas columnas para registrar la fecha del fichero y el número de bicis no disponibles
    df = df.withColumn("capacity", F.col("capacity").cast(IntegerType())) \
                            .withColumn("num_bikes_available", F.col("num_bikes_available").cast(IntegerType())) \
                            .withColumn("num_docks_available", F.col("num_docks_available").cast(IntegerType()))

    df = df \
        .withColumn("file_date", F.lit(str_date)) \
        .withColumn("num_bikes_error", 
                    F.when(F.col("capacity")>0, F.col("capacity") - (F.col("num_bikes_available") + F.col("num_docks_available")))
                    .otherwise(0)
                    ) 
    
    #Cálculo de la demanda en referncia al punto anterior
    window_spec = Window.partitionBy("station_id").orderBy("last_reported_local")
    df = df \
        .withColumn("num_bikes_available_lag", F.lag("num_bikes_available").over(window_spec)) \
        .withColumn("diferencia_bicis", F.col("num_bikes_available") - F.col("num_bikes_available_lag")) \
        .withColumn("bikes_taken", F.when(F.col("diferencia_bicis") < 0, -F.col("diferencia_bicis")).otherwise(0)) \
        .withColumn("bikes_returned", F.when(F.col("diferencia_bicis") > 0, F.col("diferencia_bicis")).otherwise(0))
    
    return df

def merge_meteo_data(spark, df, str_date, year, mes):
    print("Merging meteo data")
    df = df \
        .withColumn("last_reported_rounded", 
                    F.from_unixtime(
                        F.unix_timestamp(F.col("last_reported_local")) - (F.unix_timestamp(F.col("last_reported_local")) % 1800)
                        ).cast("timestamp")
                    )

    variables_meteo = ["temperatura", "precipitacion", "humedad", "viento"]
    for variable in variables_meteo:
        meteo_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/meteocat/{variable}/{year}/{str(mes).zfill(2)}"
        meteo_file = f"{meteo_path}/{variable}_{str_date}.parquet"
        
        if check_file_existence_hdfs(spark, meteo_file):
            df_meteo = spark.read.parquet(meteo_file, header=True, inferSchema=True)
            df_meteo.persist()

            df_meteo = df_meteo.withColumn("dia", F.to_timestamp(F.concat(F.col("fecha"), F.lit(" "), F.col("hora")), "yyy-MM-dd HH:mm:ss"))

            df = df.join(df_meteo, df.last_reported_rounded == df_meteo.dia, "left")
            df = df.withColumnRenamed("valor", variable)

            df = df.drop("dia", "codi", "fecha", "hora")

            df_meteo.unpersist()
            del df_meteo
            spark._jvm.System.gc()
            gc.collect()
        else:
            df = df.withColumn(variable, F.lit(None).cast(DoubleType()))
    
    df = df.withColumn("weather", F.struct(F.col("temperatura"), F.col("precipitacion"), F.col("humedad"), F.col("viento")))

    return df


def merge_variables(dia, mes, year):
    spark = SparkSession.builder \
            .appName("Merge variables") \
            .getOrCreate()

    file_date = datetime(year, mes, dia)
    file_date_str = file_date.strftime('%Y_%m_%d')

    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/estat/{year}/{str(mes).zfill(2)}"
    estat_file = f"{estat_path}/{file_date_str}.parquet"

    if check_file_existence_hdfs(spark, estat_file):
        df_estat = spark.read.parquet(estat_file, header=True, inferSchema=True)
        df_estat.persist()

        # Se añade la columna de Capacidad al dataframe para futuros cálculos
        df_merged = merge_informacio(spark, df_estat, file_date_str, year, mes)
        
        #Creación de nuevas columnas con referencia a los festivos y días laborables
        df_merged = merge_festivos(spark, df_merged)

        #Creación de columnas adicionales con valores calculados
        df_merged = add_calc_variables(df_merged, file_date_str)
        
        #Incorporación de las columnas con los datos meterológicos
        df_merged = merge_meteo_data(spark, df_merged, file_date_str, year, mes)

         # Almacenamos los datos de información en HDFS
        estat_path_dest = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat-variables/{year}/{str(mes).zfill(2)}"
        estat_file_dest = f"{estat_path_dest}/{file_date_str}.parquet"

        df_merged.write.mode("overwrite").parquet(estat_file_dest)
        print(f"File saved in HDFS: {estat_file_dest}")
        
        df_estat.unpersist()
        df_merged.unpersist()
        del df_estat
        del df_merged
        
        spark._jvm.System.gc()
        gc.collect()
    
    print(f"Fecha: {file_date_str} procesada")
    spark.stop()
    spark._jvm.System.gc()

def generar_documento_final(dia, mes, year):
    spark = SparkSession.builder \
            .appName("Generate documents") \
            .getOrCreate()

    file_date = datetime(year, mes, dia)
    file_date_str = file_date.strftime('%Y_%m_%d')

    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat-variables/{year}/{str(mes).zfill(2)}"
    estat_file = f"{estat_path}/{file_date_str}.parquet"

    if check_file_existence_hdfs(spark, estat_file):
        df_estat = spark.read.parquet(estat_file, header=True, inferSchema=True)
        df_estat.persist()

        # Aumentamos la granuralidad de los datos para poderlos guardar en MongoDB

        # Granularidad de 30 - no es posible
        # window_spec = Window.partitionBy("station_id", "date_reported", "last_reported_rounded").orderBy("last_reported_local")
        # df_merged = df_merged.withColumn("row_number", F.row_number().over(window_spec))
        # df_merged_filtered = df_merged.filter(F.col("row_number") == 1).drop("row_number")

        # Granularidad de 1h
        print("Aumentando granularidad")
        df_grouped = df_estat.groupBy("station_id", "date_reported", "hora_dia").agg(
            F.sum("bikes_taken").cast(IntegerType()).alias("total_bikes_taken"),
            F.sum("bikes_returned").cast(IntegerType()).alias("total_bikes_returned"),
            F.max("last_reported_local").alias("last_reported_local")
        )
        df_final = df_estat.join(
            df_grouped, 
            on=["station_id", "date_reported", "hora_dia", "last_reported_local"], 
            how="inner"
        )

        # Agrupar por estación y día, y almacenar los estados en una lista (array) de registros por intervalo de tiempo
        # df_estat_ag = df_final.groupBy("station_id", "date_reported").agg(
        #     F.sort_array(
        #         F.collect_list(
        #             F.struct("hora_dia","last_reported_local","num_bikes_available", "num_mechanical_available", "num_ebike_available",
        #                      "num_docks_available","num_bikes_error","total_bikes_taken","total_bikes_returned","status","festivo","weekday",
        #                      "laborable","weather","file_date")
        #         ), asc=True
        #     ).alias("status")
        # )
        print("Generando df final")
        df_estat_ag = df_final.groupBy("station_id", "date_reported").agg(
            F.sort_array(
                F.collect_list(
                    F.struct("hora_dia","last_reported_local","num_bikes_available", "num_docks_available","num_bikes_error",
                             "total_bikes_taken","total_bikes_returned","status","festivo","weekday","laborable","weather")
                ), asc=True
            ).alias("status")
        )
        df_estat_ag = df_estat_ag.filter(F.size(F.col("status")) > 1)

        # Almacenamos los datos de información en HDFS
        estat_path_dest = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat-final/{year}/{str(mes).zfill(2)}"
        estat_file_dest = f"{estat_path_dest}/{file_date_str}.parquet"

        df_estat_ag.write.mode("overwrite").parquet(estat_file_dest)
        print(f"File saved in HDFS: {estat_file_dest}")

        df_estat.unpersist()
        df_grouped.unpersist()
        df_final.unpersist()
        df_estat_ag.unpersist()
        del df_estat
        del df_grouped
        del df_final
        del df_estat_ag
        
        spark._jvm.System.gc()
        gc.collect()

    print(f"Fecha: {file_date_str} procesada")
    spark.stop()
    spark._jvm.System.gc()


def save_estado_to_Mongo(dia, mes, year):
    # Conexión a la base de datos de MongoDB
    mongo_uri = "mongodb://mongodb:27017/bicing_db.estado_estaciones"
    spark = SparkSession.builder \
        .appName("SparkToMongo") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.output.batchSize", "128") \
        .config("spark.mongodb.output.writeConcern.w", "majority") \
        .getOrCreate()
    
    file_date = datetime(year, mes, dia)
    file_date_str = file_date.strftime('%Y_%m_%d')
    print(f"Estado: procesando fecha {file_date_str}")

    # Creación colección informacion_estaciones
    estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat/{year}/{str(mes).zfill(2)}"
    estat_file = f"{estat_path}/{file_date_str}.parquet"
    
    if check_file_existence_hdfs(spark, estat_file):
        df_estat = spark.read.parquet(estat_file, header=True, inferSchema=True)
        df_estat.persist()
        
        print(f'count of lines: {df_estat.count()}')

        df_estat.write \
            .format("mongodb") \
            .mode("append") \
            .save()
        
        df_estat.unpersist()
        del df_estat
    
    print(f"Estado: fecha {file_date_str} procesada")
    # Stop the Spark session
    spark.stop()
    spark._jvm.System.gc()
    gc.collect()


if __name__ == "__main__":
    #showFormattedData()

    fecha_inicio = datetime(2023, 10, 10)
    #fecha_fin = datetime(2021, 1, 1)
    fecha_fin = datetime(2024, 7, 31)

    dia_actual = fecha_inicio
    while dia_actual <= fecha_fin:
        print(f"Procesando fecha: {dia_actual}")
        dia = dia_actual.day
        mes = dia_actual.month
        year = dia_actual.year
        merge_variables(dia,mes,year)
        time.sleep(2)
        generar_documento_final(dia, mes, year)
        #save_informacio_to_Mongo(dia, mes, year)
        #time.sleep(1)
        #save_estado_to_Mongo(dia, mes, year)
        time.sleep(1)

        dia_actual += timedelta(days=1)

    print(f"Proceso trusted-zone finalizado correctamente")