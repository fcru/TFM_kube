import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

def format_bcn_data(output_base_dir,hdfs_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Format State Stations Files in HDFS") \
        .getOrCreate()

    sc = spark.sparkContext
    fs = get_HDFS_FileSystem(sc)

    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path


    all_files = get_FileList_From_HDFS_Path(fs, Path, hdfs_path)

    for file in all_files:
        file_name = os.path.basename(file)
        df = spark.read.parquet(file)
        print(f"Precessing {file_name} ...")
        if file_name == "Large_shopping_centers.json" or file_name == "cultural_points_of_interests.json" or file_name == "educative_centers.json":
            category= os.path.splitext(file_name)[0]
            category = category.replace("_"," ")
            df_extracted = df.select(
                F.col("name"),
                F.col("geo_epgs_4326_latlon.lat").alias("latitude"),
                F.col("geo_epgs_4326_latlon.lon").alias("longitude"),
                F.expr("filter(addresses, a -> a.main_address = true)[0].address_name").alias("address_name"),
                F.expr("filter(addresses, a -> a.main_address = true)[0].start_street_number").alias(
                    "start_street_number"),
                F.expr("filter(addresses, a -> a.main_address = true)[0].zip_code").alias("zip_code"),
                F.lit(category).alias("category")
            )

            # Remove rows where address fields are null (i.e., no main address)
            df_final = df_extracted.filter(
                (F.col("name").isNotNull()) &
                (F.col("latitude").isNotNull()) &
                (F.col("longitude").isNotNull())
            )

            # Show the result
            df_final.show(truncate=False)
        elif file_name == "commercial_census.json":
            df_extracted = df.select(
                F.explode("features").alias("feature")
            ).select(
                F.col("feature.properties.Nom_Activitat").alias("activity_name"),
                F.col("feature.properties.Longitud").cast("double").alias("longitude"),
                F.col("feature.properties.Latitud").cast("double").alias("latitude"),
                F.lit("Commercial Activities").alias("category")
            )

            # Remove rows where required fields are null
            df_final = df_extracted.filter(
                (F.col("activity_name").isNotNull()) &
                (F.col("longitude").isNotNull()) &
                (F.col("latitude").isNotNull())
            )
            df_final.show(truncate=False)
        else:
            df_final = df
        final_path = f"{output_base_dir}/{file_name}"
        df_final.write.mode("overwrite").parquet(final_path)
        print("processed")
        time.sleep(5)


if __name__ == "__main__":
    output_base_TMB = "hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/TMB"
    hdfs_path = f"/landing-zone/batch/TMB/"
    format_bcn_data(output_base_TMB, hdfs_path)

    output_base_bcn = "hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/bcn_data"
    hdfs_path = f"/landing-zone/batch/bcn_data/"
    format_bcn_data(output_base_bcn, hdfs_path)