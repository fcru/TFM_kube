import os,sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import*

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Check All Files in HDFS") \
        .getOrCreate()

    sc = spark.sparkContext

    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(URI("hdfs://hadooop-hadoop-hdfs-nn:9000"), Configuration())

       ## Define the HDFS base directory to scan
    hdfs_path = f"/landing-zone/batch/"

    ## Get all files in the HDFS directory
    all_files = list_files_by_condition(hdfs_path,fs,Path)

    hdfs_base_dir= "hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch"

    for file_path in all_files:
        print(f"Processing file: {file_path}")
        parent_directory = os.path.dirname(file_path)
        file_name = os.path.relpath(file_path, parent_directory)
        print(f"filename : {file_name}")
        if 'ESTACIONS' in file_name or 'INFORMACIO' in file_name or 'INFO' in file_name or 'STAT' in file_name:
            print("Estacions or Informacio in pathfile")
            year = file_name [:4]
            month = file_name [5:7]
            df = spark.read.parquet(file_path, header=True, inferSchema=True)
            #df = df.withColumn("last_reported", F.col("last_reported").cast("int"))
            #df.printSchema()
            parent_directory = os.path.dirname(file_path)
            complementary_path = os.path.relpath(parent_directory, hdfs_base_dir)
            output_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/{complementary_path}/year={year}/month={month}"
            df.write.mode("overwrite").parquet(output_path)
        else:
            complementary_path = os.path.relpath(file_path, hdfs_base_dir)
            output_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/{complementary_path}"
            df = spark.read.parquet(file_path)
            df.write.mode("overwrite").parquet(output_path)
            # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()