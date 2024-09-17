import os,sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

sys.path.append(os.path.dirname(os.path.dirname((os.path.abspath(__file__)))))

from utilities import*


# Function to select common columns and fill missing columns with 'NA'
def align_dataframes(df1: DataFrame, df2: DataFrame, full_schema) -> (DataFrame, DataFrame):
    common_columns = list(set(df1.columns).intersection(set(df2.columns)))

    # Select only common columns for comparison
    df1_common = df1.select(common_columns)
    df2_common = df2.select(common_columns)

    return df1_common, df2_common

def consolidate_estacio_file():
    spark = SparkSession.builder \
        .appName("Consolidate Parquet Changes") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    sc = spark.sparkContext

    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(URI("hdfs://hadooop-hadoop-hdfs-nn:9000"), Configuration())

    ## Define the HDFS base directory to scan
    hdfs_path = "/formatted-zone/informacio/"

    ## Get all files in the HDFS directory
    partition_paths = list_files_by_condition(hdfs_path, fs, Path, condition='partition')

    # Sort partition paths to ensure chronological order
    partition_paths = sorted(partition_paths)
    # Group partition paths by year
    partition_paths_by_year = {}
    for partition_path in partition_paths:
        year = os.path.basename(os.path.dirname(partition_path)).split('=')[1]
        month = os.path.basename(partition_path).split('=')[1]
        if year not in partition_paths_by_year:
            partition_paths_by_year[year] = {}
        if month not in partition_paths_by_year[year]:
            partition_paths_by_year[year][month] = []
        partition_paths_by_year[year][month].append(partition_path)

    print(partition_paths_by_year)

    base_path = 'hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/'
    discrepancy_base_path = 'hdfs://hadooop-hadoop-hdfs-nn:9000/discrepancies/'

    def align_dataframes(df1, df2, full_schema):
        # Select only common columns for comparison
        df1_c = df1.select('id', 'capacity')
        df2_c = df2.select('id', 'capacity')
        return df1_c, df2_c

    for year, monthly_paths in partition_paths_by_year.items():
        # Load the last month's data for this year to use as the baseline schema
        last_month = sorted(monthly_paths.keys())[-1]
        df_last_partition = spark.read.parquet(monthly_paths[last_month][-1])
        full_schema = df_last_partition.schema

        # Write the last month's data as the consolidated file
        df_last_partition.write.parquet(f'{base_path}/{year}/{last_month}', mode='overwrite')
        print("df_last_partition written")
        for month, paths in monthly_paths.items():
            # Skip the last month since it's already used
            if month == last_month:
                continue

            # Load the current month's partition into a DataFrame
            df_current = spark.read.parquet(paths[0])

            # Persist DataFrames to cache them in memory
            df_last_partition.persist()

            # Align both DataFrames to the common schema
            df_current_common, df_last_common = align_dataframes(df_current, df_last_partition, full_schema)

            # Identify discrepancies (rows in df_current not in df_last)
            discrepancies_df = df_current_common.subtract(df_last_common)

            # Persist the discrepancies DataFrame to manage memory
            discrepancies_df.persist()

            # Check if there are discrepancies to write
            if discrepancies_df.count() > 0:
                # Write discrepancies to the monthly partition file
                df_current.write.parquet(f'{base_path}/{year}/{month}', mode='overwrite')
                print(f"Year {year}, Month {month}: Discrepancies found.")
            else:
                print(f"Year {year}, Month {month}: No discrepancies found.")

            # Unpersist DataFrames after processing
            df_last_partition.unpersist()


if __name__ == "__main__":
    consolidate_estacio_file()