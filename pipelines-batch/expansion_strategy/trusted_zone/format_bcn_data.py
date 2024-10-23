import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *


def calculate_null_percentages(df):
    """Calculate percentage of null values for each column in the DataFrame."""
    total_rows = df.count()
    if total_rows == 0:
        return None

    null_counts = []
    for column in df.columns:
        null_count = df.filter(F.col(column).isNull()).count()
        percentage = (null_count / total_rows) * 100
        null_counts.append((column, percentage))

    print("\nNull Value Percentages:")
    for column, percentage in null_counts:
        print(f"{column}: {percentage:.2f}%")
    return null_counts

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
        print("Original DataFrame null percentages:")
        calculate_null_percentages(df)

        if file_name == "Large_shopping_centers.json" or file_name == "cultural_points_of_interests.json" or file_name == "educative_centers.json":
            category= os.path.splitext(file_name)[0]
            category = category.replace("_"," ")
            df_extracted = df.select(
                F.col("name"),
                F.lit(category).alias("category"),
                F.concat(F.lit("POINT("), F.col("geo_epgs_4326_latlon.lon"), F.lit(" "),
                         F.col("geo_epgs_4326_latlon.lat"), F.lit(")")).alias("geometry")  # WKT format
            )

            # Remove rows where address fields are null (i.e., no main address)
            df_final = df_extracted.filter(
                (F.col("name").isNotNull()) &
                (F.col("geometry").isNotNull())
            )

        elif file_name == "commercial_census.json":
            df_extracted = df.select(
                F.explode("features").alias("feature")
            ).select(
                F.col("feature.properties.Nom_Activitat").alias("name"),
                F.lit("Commercial Activities").alias("category"),
                F.concat(F.lit("POINT("), F.col("feature.properties.Longitud"), F.lit(" "),
                         F.col("feature.properties.Latitud"), F.lit(")")).alias("geometry")  # WKT format
            )

            # Remove rows where required fields are null
            df_final = df_extracted.filter(
                (F.col("name").isNotNull()) &
                (F.col("geometry").isNotNull())  # geometry replaces latitude and longitude
            )

        elif file_name == "bcn_census_areas.json" or file_name == "bcn_neighbourhood.json":
            # Drop the 'geometria_etrs89' column
            df = df.drop("geometria_etrs89")
            df_final = df.select(
                F.col("geometria_wgs84").alias("geometry"),
                F.col("nom_barri").alias("name"),
            )

        elif file_name == "bus_stops.json":
            df_final = df.select(
                F.explode("features").alias("features")
            ).select(
                F.col("features.geometry").alias("geometry"),
                F.col("features.properties.NOM_PARADA").alias("name"),
                F.col("features.properties.DESC_PARADA").alias("stop_description")
            )

        elif file_name == "bus_lines.json" or file_name == "metro_lines.json":
            df_final = df.select(
                F.explode("features").alias("features")
            ).select(
                F.col("features.geometry").alias("geometry"),
                F.col("features.properties.NOM_LINIA").alias("name"),
                F.col("features.properties.DESC_LINIA").alias("line_description")
            )

        elif file_name == "metro_stations.json":
            df_final = df.select(
                F.explode("features").alias("features")
            ).select(
                F.col("features.geometry").alias("geometry"),
                F.col("features.properties.NOM_ESTACIO").alias("name"),
                F.col("features.properties.PICTO").alias("metro_line")
            )

        elif file_name == "bcn_no_motorized_vehicles.json" or file_name == "bcn_bike_lanes.json":
            df_final = df.select(
                F.explode("features").alias("features")
            ).select(
                F.col("features.geometry").alias("geometry"),
                F.col("features.properties.TOOLTIP").alias("name"),
            )
        else:
            df_final = df
            df.printSchema()

        print("\nProcessed DataFrame null percentages:")
        calculate_null_percentages(df_final)

        final_path = f"{output_base_dir}/{file_name}"
        df_final.write.mode("overwrite").parquet(final_path)
        df_final.show()
        print("processed")
        time.sleep(5)


    if __name__ == "__main__":
        output_base_TMB = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/TMB"
        hdfs_path = f"/landing-zone/batch/TMB/"
        format_bcn_data(output_base_TMB, hdfs_path)

        output_base_bcn = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/bcn_data"
        hdfs_path = f"/landing-zone/batch/bcn_data/"
        format_bcn_data(output_base_bcn, hdfs_path)