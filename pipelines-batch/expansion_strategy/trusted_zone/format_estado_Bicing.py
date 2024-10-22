import sys
import os
import gc
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *
from formatted_functions import *

def formatBicingState():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Format State Stations Files in HDFS") \
        .getOrCreate()
    
    sc = spark.sparkContext
    fs = get_HDFS_FileSystem(sc)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    hdfs_path = f"/landing-zone/batch/estat_estacio/"
    output_base_dir = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat_estacio"

    all_files = get_FileList_From_HDFS_Path(fs, Path, hdfs_path)

    for file in all_files:
        file_name = os.path.basename(file)
        
        #revisar que el fichero sea de ESTADO de las ESTACIONES
        if any(keyword in file_name for keyword in ['STAT','ESTACIONS']):
            year = file_name [:4]
            month = file_name[5:7]

            # Descartamos los ficheros correspondientes al 2020 y anteriores
            if int(year) > 2020:
                if check_file_existence_hdfs(spark,f"{output_base_dir}/{year}/{month}"):
                    print(f"File: {file} is already processed")
                else:
                    print(f"Processing file: {file}")

                    #Lectura del contenido del fichero. Obtenemos los datos de un mes
                    df = spark.read.parquet(file, header=True, inferSchema=True)

                    df.persist()

                    # Selección de las columnas deseadas
                    df_selected = df.select(
                        "station_id",
                        "num_bikes_available",
                        F.col("`num_bikes_available_types.mechanical`").alias("num_mechanical_available"),
                        F.col("`num_bikes_available_types.ebike`").alias("num_ebike_available"),
                        "num_docks_available",
                        F.from_unixtime(F.col("last_reported")).cast("timestamp").alias("last_reported_GMT"),
                        "is_charging_station",
                        "is_installed",
                        "is_renting",
                        "is_returning",
                        "status",
                        F.from_unixtime(F.col("last_updated")).cast("timestamp").alias("last_updated_GMT")
                    )

                    # Extraer únicamente la fecha y hora de la columna last_updated en su conversion a GMT+ según zona horaria local
                    df_with_date = df_selected \
                        .withColumn("last_reported_local", F.from_utc_timestamp(df_selected["last_reported_GMT"], "Europe/Madrid")) \
                        .withColumn("last_updated_local", F.from_utc_timestamp(df_selected["last_updated_GMT"], "Europe/Madrid")) \
                        .withColumn("date_reported", F.to_date("last_reported_local")) \
                        .withColumn("hour_reported", F.date_format("last_reported_local", "HH:mm:ss")) \
                        .withColumn("date", F.to_date("last_updated_local")) \
                        .withColumn("hour", F.date_format("last_updated_local", "HH:mm:ss"))
                    
                    # Se guardan los datos del datframe en parquet en HDFS.
                    # Los ficheros los ubicamos en el directorio estat/año/mes
                    # separamos los ficheros con el objetivo de tener un fichero por día
                    unique_days = df_with_date.select("date").distinct().collect()
                    for date_row in unique_days:
                        date = date_row["date"]
                        print(date)
                        
                        if date is not None:
                            date_str = date.strftime('%Y_%m_%d')
                            output_path = f"{output_base_dir}/{year}/{month}/{date_str}.parquet"

                            df_day = df_with_date.filter(df_with_date["date"] == date)
                            df_day.write.mode("overwrite").parquet(output_path)

                            print(f"File saved in HDFS: {output_path}")

                            # liberamos memoria del dataframe diario
                            df_day.unpersist()
                            del df_day
                            
                            gc.collect()
                            spark._jvm.System.gc()
                            time.sleep(5)
                    
                    # liberamos memoria del os dataframes generales antes de leer un nuevo fichero
                    df_with_date.unpersist()
                    del df_with_date

                    df_selected.unpersist()
                    del df_selected

                    df.unpersist()
                    del df
                    
                    gc.collect()
                    spark._jvm.System.gc()
                    time.sleep(5)
    
    spark.stop()
    
    # Forzamos la liberación de memoria de Spark
    spark._jvm.System.gc()

def validar_ficheros_por_fecha():
    spark = SparkSession.builder \
        .appName("join files Estado") \
        .getOrCreate()
    
    years = [2021, 2022, 2023, 2024]

    for year in years:
        for mes in range(1,13):
            if year == 2024 and mes > 7:
                break

            estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat_estacio/{year}/{str(mes).zfill(2)}"
            
            previous_day, previous_month, previous_year = get_day_previous_month(year, mes)
            previous_date = datetime(previous_year, previous_month, previous_day)
            previous_date_str = previous_date.strftime('%Y_%m_%d')
            
            date = datetime(year,mes,1)            
            date_str = date.strftime('%Y_%m_%d')

            previous_file = f"{estat_path}/{previous_date_str}.parquet"
            file = f"{estat_path}/{date_str}.parquet"

            if check_file_existence_hdfs(spark, previous_file):
                df_previous = spark.read.parquet(previous_file, header=True, inferSchema=True)
                df_previous.persist()
                num_filas_prev = df_previous.count()
                print(f"num filas previous: {num_filas_prev}")
                
                df_actual = spark.read.parquet(file, header=True, inferSchema=True)
                df_actual.persist()
                num_filas_actual = df_actual.count()
                print(f"num filas first: {num_filas_actual}")
                
                df = df_previous.union(df_actual)
                num_filas = df.count()
                print(f"num filas total: {num_filas}")

                df.write.mode("overwrite").parquet(file)
                print(f"File saved in HDFS: {file}")

                if delete_file_hdfs(spark, previous_file): print(f"File {previous_file} has been deleted")
                else: print(f"It's not possible delete file {previous_file}")

                df_previous.unpersist()
                df_actual.unpersist()
                df.unpersist()

                del df_previous
                del df_actual
                del df

                gc.collect()
                spark._jvm.System.gc()

                time.sleep(1)
            
            next_month, next_year = get_next_month(year, mes)
            next_date = datetime(next_year, next_month, 1)
            next_date_str = next_date.strftime('%Y_%m_%d')

            next_file = f"{estat_path}/{next_date_str}.parquet"

            last_date = datetime(year, mes, calendar.monthrange(year, mes)[1])
            last_date_str = last_date.strftime('%Y_%m_%d')
            last_file = f"{estat_path}/{last_date_str}.parquet"

            if check_file_existence_hdfs(spark, next_file):
                df_next = spark.read.parquet(next_file, header=True, inferSchema=True)
                df_next.persist()
                num_filas_next = df_next.count()
                print(f"num filas next: {num_filas_next}")
                
                df_last = spark.read.parquet(last_file, header=True, inferSchema=True)
                df_last.persist()
                num_filas_last = df_last.count()
                print(f"num filas last: {num_filas_last}")
                
                df = df_next.union(df_last)
                num_filas = df.count()
                print(f"num filas total: {num_filas}")

                df.write.mode("overwrite").parquet(last_file)
                print(f"File saved in HDFS: {last_file}")

                if delete_file_hdfs(spark, next_file): print(f"File {next_file} has been deleted")
                else: print(f"It's not possible delete file {next_file}")

                df_next.unpersist()
                df_last.unpersist()
                df.unpersist()

                del df_next
                del df_last
                del df

                gc.collect()
                spark._jvm.System.gc()

                time.sleep(1)


    spark.stop()
    # Forzamos la liberación de memoria de Spark
    spark._jvm.System.gc()

if __name__ == "__main__":
    formatBicingState()
    gc.collect()
    validar_ficheros_por_fecha()
    gc.collect()