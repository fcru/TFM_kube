# Para los dato del meteocat, leemos los ficheros JSON y les damos formato de tabla para guardarlos 
# en formato Parquet en HDFS.
# Guardamos un fichero por día en el directorio demanda/meteocat/variable/año/mes
# Se da formato timestamp a la columna fecha y los valores de lectura de la variable 

import sys
import os
import time
import gc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

def formatMeteocatData(variable):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Format Meteocat data") \
        .getOrCreate()
    
    sc = spark.sparkContext
    fs = get_HDFS_FileSystem(sc)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    hdfs_path = f"/landing-zone/batch/METEOCAT/{variable}"
    output_base_dir = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/meteocat/{variable}"

    all_files = get_FileList_From_HDFS_Path(fs, Path, hdfs_path)
    
    # en el nombre del fichero hay variable_año_mes_dia
    for file in all_files:
        file_name = os.path.basename(file)
        file_parts = file_name.split('_')

        year = file_parts[1]
        month = file_parts[2]
        day = file_parts[3].split('.')[0]
        
        if int(year) > 2020:
            output_path = f"{output_base_dir}/{year}/{month}/{variable}_{year}_{month}_{day}.parquet"
            if check_file_existence_hdfs(spark,output_path):
                print(f"File: {file_name} is already processed")
            else:
                print(f"Processing file: {file}")

                try:
                    df = spark.read.json(file)

                    df.persist()

                    df_exploded = df.withColumn("lectura", F.explode("lectures")).select("codi", "lectura.*")
                    df_final = df_exploded \
                        .withColumn("data_timestamp", F.to_timestamp("data", "yyyy-MM-dd'T'HH:mm'Z'")) \
                        .withColumn("fecha", F.to_date("data_timestamp")) \
                        .withColumn("hora", F.date_format("data_timestamp", "HH:mm:ss")) \
                        .withColumn("valor", F.round("valor", 1)) \
                        .select("codi", "fecha", "hora", "valor")

                    df_final.write.mode("overwrite").parquet(output_path)
                    print(f"File saved in HDFS: {output_path}")

                    df.unpersist(blocking=True)
                    df_exploded.unpersist()
                    df_final.unpersist()

                    del df
                    del df_exploded
                    del df_final
                except Exception as e:
                    print(f"File {file_name} has an error and it is not read")

                finally:
                    # Forzamos la liberación de memoria de Spark
                    spark._jvm.System.gc()

                    time.sleep(1)

    spark.stop()

    spark._jvm.System.gc()

if __name__ == "__main__":
    formatMeteocatData("temperatura")
    gc.collect()
    formatMeteocatData("precipitacion")
    gc.collect()
    formatMeteocatData("humedad")
    gc.collect()
    formatMeteocatData("viento")
    gc.collect()