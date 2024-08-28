import os,sys

from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, max as spark_max, dayofmonth, month, year, collect_list, struct
from pyspark.sql.types import *
from pyspark.sql import SparkSession


sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))

from utilities import *


def convert_dates_to_iso(path,spark):
    # Initialize Spark session
    try:
        # Read all parquet files partitioned by year and month
        df = spark.read.parquet(path)
        if 'last_reported' in df.columns:
            # Convert 'last_reported' to ISO format
            df = df.withColumn("last_reported_iso", F.from_unixtime(F.col("last_reported"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        return df

    except Exception as e:
        print(f"Error reading or processing {path}: {e}")
        raise e

def convert_informacio_estacio_dates(hdfs_path,output_base_path):

    spark = SparkSession.builder \
        .appName("Consolidate Parquet Changes") \
         .getOrCreate()

    sc = spark.sparkContext

    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(URI("hdfs://hadooop-hadoop-hdfs-nn:9000"), Configuration())

    ## Get all files in the HDFS directory
    partition_paths = list_files_by_condition(hdfs_path, fs, Path, condition='partition')

    # Sort partition paths to ensure chronological order
    partition_paths = sorted(partition_paths)
    base_path = 'hdfs://hadooop-hadoop-hdfs-nn:9000'
    for path in partition_paths:
        try:
            parent_directory = os.path.dirname(f'{base_path}/{hdfs_path}')
            complementary_path = os.path.relpath(path, parent_directory)
            output_path = f'{output_base_path}/{complementary_path}'
            print(f'output_path: {output_path}')
            if check_file_existence_hdfs(spark, output_path):
                print(f"process cancelled, file {output_path} already exists")
            else:
                df = convert_dates_to_iso(path,spark)
                df.write.mode('overwrite').parquet(output_path)
                print(f"file written at {output_path}")
        except Exception as e:
            print(f"Error processing {path}: {e}")
            continue
    spark.stop()

def calculate_capacity_by_station(df):
    capacity_mode_df = df.groupBy("station_id", "capacity").agg(F.count("*").alias("count"))
    #capacity_mode_df.show()
    # Determine the maximum count for each station_id
    max_count_df = capacity_mode_df.groupBy("station_id").agg(F.max("count").alias("max_count"))
    # Join the two DataFrames to filter rows where the count is equal to the maximum count
    mode_df = capacity_mode_df.join(max_count_df, on=["station_id"]) \
        .filter(capacity_mode_df["count"] == max_count_df["max_count"]) \
        .select("station_id", "capacity")

    # If there are ties (multiple modes), this will select the first mode for each station_id
    mode_df = mode_df.groupBy("station_id").agg(F.first("capacity"))

    return mode_df


def set_capacity(informacio_folder, output_folder):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("JoinCapacity") \
        .getOrCreate()

    sc = spark.sparkContext

    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(URI("hdfs://hadooop-hadoop-hdfs-nn:9000"), Configuration())
    info_paths = list_files_by_condition(informacio_folder, fs, Path, condition='partition')

    info_paths = sorted(info_paths)

    # Dictionary to hold monthly capacities by year
    schema = StructType([
        StructField("station_id", StringType(), True),
        StructField("capacity", IntegerType(), True),
        StructField("year", StringType(), True)
    ])
    final_modes_df = spark.createDataFrame([], schema=schema)

    for year in sorted(set(info_path.split("year=")[-1].split("/")[0] for info_path in info_paths)):
        print(f"processing year: {year}")
        monthly_schema = StructType([
            StructField("station_id", StringType(), True),
            StructField("capacity", IntegerType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
        ])
        all_monthly_modes_df = spark.createDataFrame([], schema=monthly_schema)
        for info_path in info_paths:
            if f"year={year}" in info_path and int(year) > 2020:
                # Extract year and month from the file paths
                month = info_path.split("month=")[-1].split("/")[0]
                year_month = f"year={year}/month={month}"
                      # avoid 03/2022 file because it has a wrong structure
                if year_month != "year=2022/month=03":
                    # Load the informacio DataFrames for the current month
                    df_informacio = spark.read.parquet(info_path).select("station_id", "capacity")
                    # Calculate the mode of capacity
                    # Group by station_id and capacity, and count occurrences
                    mode_df = calculate_capacity_by_station(df_informacio)
                    mode_df = mode_df.withColumn("year", F.lit(year))
                    mode_df = mode_df.withColumn("month", F.lit(month))
                    # Save the mode capacity for the current month
                    all_monthly_modes_df=all_monthly_modes_df.union(mode_df)
        yearly_mode_df = calculate_capacity_by_station(all_monthly_modes_df)
        # Add the year to the final mode DataFrame
        yearly_mode_df = yearly_mode_df.withColumn("year", F.lit(year))
            # Keep all columns except 'capacity' from last_month_df
        # Append the yearly mode results to final_modes_df
        final_modes_df = final_modes_df.union(yearly_mode_df)
    final_modes_df.write.mode('overwrite').parquet(output_folder)

    # Stop the SparkSession
    spark.stop()

def add_fields_to_capacity(capacity_path, info_path_last_file, capacity_path_output):
    spark = SparkSession.builder \
        .appName("JoinCapacity") \
        .getOrCreate()
    df_informacio = spark.read.parquet(capacity_path)
    print(f'df initial lines {df_informacio.count()}')
    last_month_df = spark.read.parquet(info_path_last_file)
    last_month_df = last_month_df.drop("capacity")
    last_month_df = last_month_df.dropDuplicates(["station_id"])
    yearly_mode_df = df_informacio.join(last_month_df, on="station_id", how="left")
    print(f'final df lines: {yearly_mode_df.count()}')
    yearly_mode_df.write.mode('overwrite').parquet(capacity_path_output)
    # Stop the SparkSession
    spark.stop()

def create_mongodb_bicing(estat_path,capacity_path):
    mongo_uri = "mongodb://mongodb:27017/bicing_db.estacio"
    spark = SparkSession.builder \
        .appName("SparkToMongo") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.output.writeConcern.w", "majority") \
        .getOrCreate()
    df = spark.read.parquet(capacity_path)
    df.show()
    print(f'count of lines: {df.count()}')
    print(f'count of distinct lines: {df.distinct().count()}')
    df.write \
        .format("mongodb") \
        .mode("append") \
        .save()

    # Stop the Spark session
    spark.stop()

def verify_mongo_writing():
    # Connect to MongoDB
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['bicing_db']
    collection = db['estacio']
    count_docs = collection.count_documents({})
    # Print the count
    print(f"Number of documents in '{collection}': {count_docs}")
    # Query the collection
    documents = collection.find().limit(10)
    for doc in documents:
        print(doc)
    # Close the connection
    client.close()