import os,sys
import time
import traceback
import pyarrow.fs as fs

from pymongo import MongoClient
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import collect_list, struct, hour, expr, to_date, date_format
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col
from datetime import datetime, timedelta


sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))
from utilities import *

mongo_uri = "mongodb://mongodb:27017/bicing_db.estacio"

def inspect_files():
    spark = SparkSession.builder \
        .appName("InspectFiles") \
        .getOrCreate()

    df_informacio = spark.read.parquet(
        f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/informacio/2023/05/2023_05_15.parquet")

    df_estat = spark.read.parquet(
        f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/estat_estacio/2023/05/2023_05_15.parquet")

    df_info_c = spark.read.parquet(
        "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio_consolidate")

    print('informacio:')
    print(df_informacio.printSchema())
    df_informacio.show(5)

    print('estat:')
    print(df_estat.printSchema())
    df_estat.show(5)

    print('consolidate_informacio:')
    print(df_info_c.printSchema())
    df_info_c.show(5)




# Define the consolidation process
def consolidate_informacio(spark, dia_actual, df_final=None):
    dia = dia_actual.day
    mes = dia_actual.month
    year = dia_actual.year
    file_date = datetime(year, mes, dia)
    file_date_str = file_date.strftime('%Y_%m_%d')
    info_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/informacio/{year}/{str(mes).zfill(2)}"
    info_file = f"{info_path}/{file_date_str}.parquet"

    print(f"Processing {info_file}")

    # Read the new file if it exists
    if check_file_existence_hdfs(spark, info_file):
        df_informacio = spark.read.parquet(info_file)
        df_informacio = df_informacio.drop('max_capacity', 'date') \
                                    .withColumn("station_id", df_informacio["station_id"].cast("string").cast("int")) \
                                    .distinct()
        df_informacio = df_informacio.na.drop()

        # If no previous df_final, just return df_informacio as the new df_final
        if df_final is None or df_final.rdd.isEmpty():
            return df_informacio

        # Add suffix to df_informacio columns (excluding station_id)
        df_informacio_renamed = df_informacio.select(
            [col('station_id')] +
            [col(c).alias(c + '_new') for c in df_informacio.columns if c != 'station_id']
        )

        # Perform the full outer join based on 'station_id'
        joined_df = df_final.join(df_informacio_renamed, "station_id", "full_outer")

        # Apply coalesce to matching columns from df_informacio and df_final
        select_expr = []
        for col_name in df_final.columns:
            if col_name != 'station_id':
                # Check if the column exists in the renamed df_informacio
                new_col_name = col_name + '_new'
                if new_col_name in df_informacio_renamed.columns:
                    select_expr.append(coalesce(col(new_col_name), col(col_name)).alias(col_name))
                else:
                    select_expr.append(col(col_name))
            else:
                select_expr.append(col("station_id"))

        # Select the columns and remove duplicates
        result_df = joined_df.select(*select_expr).distinct()
        print(f"Result count: {result_df.count()}")

        # Write the updated DataFrame back to HDFS
        output_file_path = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio_consolidate"
        result_df.write.mode("overwrite").parquet(output_file_path)

        return result_df

# Main process for consolidating data over a date range
def consolidation_process():
    spark = SparkSession.builder \
        .appName("consolidate_informacio") \
        .getOrCreate()

    fecha_inicio = datetime(2021, 2, 10)
    fecha_fin = datetime(2024, 7, 31)
    dia_actual = fecha_inicio
    output_file_path = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio_consolidate"

    # Load the final DataFrame if it exists
    if check_file_existence_hdfs(spark, output_file_path):
        df_final = spark.read.parquet(output_file_path)
    else:
        df_final = None

    # Iterate over the date range
    while dia_actual <= fecha_fin:
        print(f"Processing date: {dia_actual}")
        df_final = consolidate_informacio(spark, dia_actual, df_final)
        time.sleep(5)
        dia_actual += timedelta(days=30)

    print("Trusted-zone process completed successfully.")

# Example function to check file existence in HDFS (replace with actual implementation)
def check_file_existence_hdfs(spark, file_path):
    try:
        spark.read.parquet(file_path)
        return True
    except Exception:
        return False


def ensure_index_exists(index_field):
    client = MongoClient(mongo_uri)
    db = client["bicing_db"]
    collection = db['estacio']

    # Get the list of indexes
    existing_indexes = collection.list_indexes()
    index_names = [index['name'] for index in existing_indexes]

    index_name = f"{index_field}_1"  # Assuming ascending index; use "-1" for descending

    if index_name in index_names:
        print(f"Index '{index_name}' already exists.")
    else:
        # Create the index
        collection.create_index([(index_field, 1)])  # Change to -1 for descending
        print(f"Index '{index_name}' created on '{index_field}' field.")


def check_document_exists(mongo_uri, date):
    # Create MongoDB client
    client = MongoClient(mongo_uri)
    db = client["bicing_db"]
    collection_name = 'estacio'
    # Check if the collection exists
    if collection_name not in db.list_collection_names():
        print(f"Collection '{collection_name}' does not exist in database")
        return False

    # Perform query to check if document with specified date exists
    query = {"date": date.strftime("%Y-%m-%d")}
    result = db[collection_name].find_one(query)

    if result:
        print(f"Document with date {date.strftime('%Y-%m-%d')} exists.")
        return True
    else:
        print(f"No document found with date {date.strftime('%Y-%m-%d')}.")
        return False


def consolidate_estacio():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("SparkToMongo") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
            .config("spark.mongodb.write.connection.uri", mongo_uri) \
            .config("spark.mongodb.read.connection.uri", mongo_uri) \
            .config("spark.mongodb.output.writeConcern.w", "majority") \
            .config("spark.mongodb.output.batchSize", "128") \
            .getOrCreate()

        information_path = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/informacio_consolidate"
        df_informacio = spark.read.parquet(information_path)
        df_informacio_renamed = df_informacio.withColumnRenamed("station_id", "station_id_info")
        df_informacio_renamed.persist()

        fecha_inicio = datetime(2021, 1, 1)
        fecha_fin = datetime(2024, 7, 31)
        dia_actual = fecha_inicio

        ensure_index_exists("date")

        while dia_actual <= fecha_fin:
            #dia = dia_actual.day
            #mes = dia_actual.month
            #year = dia_actual.year
            #file_date = datetime(year, mes, dia)
            file_date_str = dia_actual.strftime('%Y_%m_%d')
            print(f"analyzing {file_date_str}")
            if check_document_exists(mongo_uri, dia_actual):
                print("document already exists for this date, skipping..")
            else:
                estat_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/estat_estacio/{dia_actual.year}/{str(dia_actual.month).zfill(2)}"
                estat_file = f"{estat_path}/{file_date_str}.parquet"
                if check_file_existence_hdfs(spark, estat_file):
                    print(f'processing {estat_file}')
                    df = spark.read.parquet(estat_file)
                    df.drop(
                    "last_reported_GMT",
                    "is_charging_station",
                    "is_installed",
                    "is_renting",
                    "is_returning",
                    "status",
                    "last_updated_GMT",
                    "last_reported_local",
                    "last_updated_local",
                    "date_reported",
                    "hour_reported")
                    df = df.withColumn("date", to_date(df.date)) \
                            .withColumn("hour", hour(df.hour))

                    group_columns = ["station_id", "date", "hour"]
                    # Columns for which we want to calculate the mode
                    mode_columns = [
                        "num_bikes_available",
                        "num_docks_available",
                    ]
                    # Window specification to find the maximum count per group
                    window_spec = Window.partitionBy(group_columns).orderBy(F.desc("count"), *mode_columns)
                    df_mode = df.groupBy(group_columns + mode_columns) \
                        .agg(F.count("*").alias("count")) \
                        .withColumn("rank", F.rank().over(window_spec)) \
                        .filter(col("rank") == 1) \
                        .select(group_columns + mode_columns)
                    join_condition = (df_informacio_renamed["station_id_info"] == df_mode["station_id"])

                    consolidate_estacio_day = df_mode.join(df_informacio_renamed, on=join_condition, how="left").drop("station_id_info")
                    # Write the resulting dataframe to the output path
                    df_grouped = consolidate_estacio_day.groupBy("station_id", "date").agg(
                        # Aggregate main document fields
                        expr("first(name) as name"),
                        expr("first(lat) as lat"),
                        expr("first(lon) as lon"),
                        expr("first(altitude) as altitude"),
                        expr("first(address) as address"),
                        expr("first(post_code) as post_code"),
                        # Collect hourly data into a list of status subdocuments
                        collect_list(struct(
                            col("hour"),
                            col("num_bikes_available"),
                            col("num_docks_available"),
                        )).alias("status")
                    )
                    # Transform the aggregated data into the MongoDB document format
                    df_transformed = df_grouped.select(
                        col("station_id"),
                        date_format("date", "yyyy-MM-dd").alias("date"),
                        col("name"),
                        col("lat"),
                        col("lon"),
                        col("altitude"),
                        col("address"),
                        col("post_code"),
                        col("status")
                    )
                    # df_transformed.printSchema()
                    # Write to MongoDB
                    df_transformed.write \
                        .format("mongodb") \
                        .mode("append") \
                        .option("uri", mongo_uri) \
                        .save()
                    time.sleep(5)
                    # Clean up
                    del df
                    del consolidate_estacio_day
                else:
                    print(f"Missing estat estacio file for {file_date_str}")
            dia_actual += timedelta(days=1)
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

    finally:
        if spark:
            df_informacio_renamed.unpersist()
            spark.stop()
            spark._jvm.System.gc()
        print("Finished processing.")


def process_json_files_to_mongodb(root_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SparkToMongo") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.mongodb.output.writeConcern.w", "majority") \
        .config("spark.mongodb.output.batchSize", "128") \
        .getOrCreate()

    try:
        sc = spark.sparkContext
        # Get list of all JSON files
        fs = get_HDFS_FileSystem(sc)
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        json_files = get_FileList_From_HDFS_Path(fs, Path, root_path)

        for file_path in json_files:
            print(f"Processing file: {file_path}")

            # Read the JSON file
            df = spark.read.parquet(file_path)
            df.printSchema()
            # Check for _corrupt_record and filter out invalid records
            if '_corrupt_record' in df.columns:
                valid_df = df.filter(df["_corrupt_record"].isNull())
                corrupt_df = df.filter(df["_corrupt_record"].isNotNull())

                if corrupt_df.count() > 0:
                    print(f"Found {corrupt_df.count()} corrupted records in {file_path}")
                    corrupt_df.show(truncate=False)

                if valid_df.count() == 0:
                    print(f"No valid records found in {file_path}, skipping file.")
                    continue  # Skip to the next file if no valid records

            else:
                valid_df = df  # If no _corrupt_record column, proceed with the entire DataFrame

            # Get the filename without extension to use as collection name
            file_name = os.path.splitext(os.path.basename(file_path))[0]

            # Write valid data to MongoDB in a collection named after the file
            valid_df.write.format("mongodb") \
                .option("collection", file_name) \
                .mode("overwrite") \
                .save()

            print(f"Processed file: {file_path}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Stop Spark session
        spark.stop()
