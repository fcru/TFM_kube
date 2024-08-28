import sys,os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def to_geometry(lat, lon, alt):
    return f"POINT ({lon} {lat} {alt})"

# Function to list files recursively
def format_geometry(spark, path, output_path):
    df = spark.read.parquet(path)
    if check_geojson_format(df):
        df.write.mode('overwrite').parquet(output_path)
    elif 'lat' in df.columns and 'lon' in df.columns and 'altitude' in df.columns:
        geo_udf = udf(to_geometry, StringType())
        df_with_geom = df.withColumn("geometry", geo_udf(col("lat"), col("lon"), col("altitude")))
        df_with_geom.write.mode('overwrite').parquet(output_path)



def check_geojson_format(df):
    contains_geo = any('geo' in col.lower() for col in df.columns)
    contains_features = any('features' in col.lower() for col in df.columns)
    if contains_geo or contains_features:
        return True
    else:
        return False


def list_csv_or_json_files(hdfs_path, fs, Path):
    file_status = fs.listStatus(Path(hdfs_path))
    files = []
    for status in file_status:
            path_dir = status.getPath().toString()
            if path_dir.endswith(".csv") or path_dir.endswith(".json"):
              files.append(status.getPath().toString())
            else:
              files.extend(list_csv_or_json_files(path_dir,fs,Path))
    return files
