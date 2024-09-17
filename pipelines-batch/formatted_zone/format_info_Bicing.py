# El objetivo de esta transformación es, de todos los ficheros con la información de las estaciones 
# del bicing, obtener un único fichero de información por día, agrupando los datos por estación,
# año, mes y día.
# - Se descartan los datos del año 2020, debido a la pandemia COVID-19.
# - Se descartan los datos del año 2019, debido al inicio del servicio y no tener todos el mismo
#   formato
# - Como todos los ficheros no tienen el mismo formato, los estandarizamos y nos quedamos con la 
#   información común para todos los ficheros. Las columnas resultantes son:
#       station_id,"name", "lat","lon","altitude","address","post_code","capacity", fecha (UTC dia/mes/año)
#   Descartamos las columnas que no cambian nunca y que no aportan información diferencial 
#   y las que siempre son NA
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

def getInfoEstacionesXDay():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Group Information Files in HDFS") \
        .getOrCreate()

    sc = spark.sparkContext
    fs = get_HDFS_FileSystem(sc)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    hdfs_path = f"/landing-zone/batch/informacio/"
    output_base_dir = "hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/demanda/informacio"

    all_files = get_FileList_From_HDFS_Path(fs, Path, hdfs_path)

    for file in all_files:
        file_name = os.path.basename(file)
               
        #revisar que el fichero sea de INFORMACION
        if any(keyword in file_name for keyword in ['INFORMACIO','INFO']):
            year = file_name [:4]
            month = file_name[5:7]

            # Descartamos los ficheros correspondientes al 2020
            if int(year) > 2020:
                if check_file_existence_hdfs(spark,f"{output_base_dir}/{year}/{month}"):
                    print(f"File: {file} is already processed")
                else:
                    print(f"Processing file: {file}")
            
                    #Lectura del contenido del fichero
                    df = spark.read.parquet(file, header=True, inferSchema=True)
                
                    # Selección de las columnas deseadas
                    df_selected = df.select(
                        "station_id",
                        "name",
                        "lat",
                        "lon",
                        "altitude",
                        "address",
                        "post_code",
                        "capacity",
                        F.from_unixtime(F.col("last_updated")).cast("timestamp").alias("last_updated_timestamp")
                    )

                    # Extraer únicamente la fecha de la columna last_updated_timestamp
                    df_with_date = df_selected.withColumn("date", F.to_date("last_updated_timestamp"))
                    
                    # GroupBy de los datos por las columnas station_id y fecha 
                    # Nos quedamos con la capacidad máxima de un día
                    df_grouped = df_with_date.groupBy("station_id", "date").agg(
                        F.first("name").alias("name"),
                        F.first("lat").alias("lat"),
                        F.first("lon").alias("lon"),
                        F.first("altitude").alias("altitude"),
                        F.first("address").alias("address"),
                        F.first("post_code").alias("post_code"),
                        F.max("capacity").alias("max_capacity")
                        #F.collect_set("capacity").alias("distinct_capacities") # todos los valores distintos de capacity
                    )

                    # Mostrar el DataFrame final
                    # df_filtered = df_grouped.filter(F.col("station_id") == "1")
                    # df_filtered.show(35)

                    # Se guardar los datos del datframe en parket en HDFS. 
                    # Los ficheros los ubicamos en el directorio información/año/mes y tendremos un
                    # fichero por cada día
                    unique_days = df_grouped.select("date").distinct().collect()
                    for date_row in unique_days:
                        date = date_row["date"]
                        print(date)
                        
                        if date is not None:
                            date_month = date.month
                            date_year = date.year
                            if date_year == int(year) and date_month == int(month):
                                df_day = df_grouped.filter(df_grouped["date"] == date)
                            
                                date_str = date.strftime('%Y_%m_%d')
                            
                                output_path = f"{output_base_dir}/{year}/{month}/{date_str}.parquet"
                                df_day.write.mode("overwrite").parquet(output_path)

                                print(f"File saved in HDFS: {output_path}")
                            else:
                                print(f"mes: {month}; date_month: {date_month}; año: {year}; date_year: {date_year}")
        
        # Forzamos la liberación de memoria de Spark
        spark._jvm.System.gc()
    spark.stop()


if __name__ == "__main__":
    getInfoEstacionesXDay()