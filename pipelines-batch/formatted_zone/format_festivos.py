# Para los festivos tenemos dos ficheros: 
#   1. El fichero general con todos los festivos de Cataluña. Seleccionamos los campos any y data. El campo data lo convertimos a fecha. 
#       Nos quedamos solo con los festivos de los años > 2020
#   2. El fichero con los festivos locales. 
#       - De todos los datos filtramos y nos quedamos solo con los que en el campo "ajuntament_o_nucli_municipal" = "Barcelona" y en el campo "festiu" = "Festa local"
#       - Seleccionamos solo los campos any_calendari y data (que la convertimos a fecha)
# De estos dos ficheros generamos uno solo juntando el resultado de ambas selecciones que guardamos en parquet

import sys
import os
import gc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

def formatHolidays():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Get Holidays File") \
        .getOrCreate()
    
    df_generales = spark.read.json("hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/festivos/festivos_generales.json")

    df_generales = df_generales.withColumn("data", F.to_date(F.col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    df_generales_final = df_generales.filter((F.col("any") > 2020) & (F.col("any") <= "2024")) \
                                     .select("any", "data")

    df_locales = spark.read.json("hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/festivos/festivos_locales.json")

    df_locales_final = df_locales.filter((F.col("any_calendari") >= "2021") & 
                                         (F.col("any_calendari") <= "2024") & 
                                         (F.col("ajuntament_o_nucli_municipal") == "Barcelona") & 
                                         (F.col("festiu") == "Festa local")) \
                                .withColumn("data", F.to_date(F.col("data"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
                                .select(F.col("any_calendari").alias("any"), "data")

    # unión de ambos dataframes en uno
    df_festivos = df_generales_final.union(df_locales_final)

    df_festivos.write.mode("overwrite").parquet("hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/festivos/holidays.parquet")
    print(f"File saved in HDFS")

    spark.stop()

if __name__ == "__main__":
    formatHolidays()