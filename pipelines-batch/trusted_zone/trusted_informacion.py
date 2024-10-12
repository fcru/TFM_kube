import sys
import os
import gc
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from trusted_demanda import obtener_fecha

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

def save_station_information(spark, str_date, year, mes):
    info_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/informacio/{year}/{str(mes).zfill(2)}"
    info_file = f"{info_path}/{str_date}.parquet"

    if check_file_existence_hdfs(spark, info_file):
        df_informacio = spark.read.parquet(info_file, header=True, inferSchema=True)
        df_informacio.persist()

        #Estructuramos el dataframe de información para dejarlo en HDFS con la forma deseada antes de traspasarlo a Mongo o guardarlo en HDFS
        df_informacio = df_informacio \
            .withColumn("station_id", F.col("station_id").cast(IntegerType())) \
            .withColumn("post_code", F.concat(F.lit('0'), F.col("post_code")).cast(StringType())) \
            .withColumn("lon", F.col("lon").cast(DoubleType())) \
            .withColumn("lat", F.col("lat").cast(DoubleType())) \
            .withColumn("altitude", F.col("altitude").cast(DoubleType())) \
            .withColumn("geoJSON_coord", F.array(F.col("lon"), F.col("lat"), F.col("altitude"))) \
            .select("station_id", "date", "name", "address", "post_code", "lon", "lat", "altitude", "geoJSON_coord", F.col("max_capacity").alias("capacity"))
        
        #Almacenamos los datos de información en HDFS
        informacio_path_dest = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio/{year}/{str(mes).zfill(2)}"
        informacio_file_dest = f"{informacio_path_dest}/{str_date}.parquet"
        
        df_informacio.write.mode("overwrite").parquet(informacio_file_dest)
        print(f"File saved in HDFS: {informacio_file_dest}")

        #Liberamos memoria
        df_informacio.unpersist()
        del df_informacio
        spark._jvm.System.gc()
        gc.collect()

if __name__ == "__main__":
    fecha_inicio = obtener_fecha("Introduce la fecha de inicio")
    fecha_fin = obtener_fecha("Introduce la fecha fin")
    if fecha_fin < fecha_inicio:
        print("La fecha de fin no puede ser anterior a la fecha de inicio.")
    else:
        spark = SparkSession.builder \
            .appName("Save InfoBicing") \
            .getOrCreate()
        
        dia_actual = fecha_inicio
        while dia_actual <= fecha_fin:
            print(f"Procesando fecha: {dia_actual}")
            dia = dia_actual.day
            mes = dia_actual.month
            year = dia_actual.year
            file_date = datetime(year, mes, dia)
            file_date_str = file_date.strftime('%Y_%m_%d')
            save_station_information(spark, file_date_str, year, mes)

            dia_actual += timedelta(days=1)
        
        spark.stop()
        
    print(f"Proceso trusted-zone finalizado correctamente")