from pyspark.sql import SparkConf, SparkContext
from datetime import datetime



def get_time_partition(timestamp):
    # Convert the timestamp to a datetime object
    date = datetime.fromtimestamp(timestamp / 1000.0)

    # Format the datetime object into the desired format
    return date.strftime("year=%Y/month=%m/day=%d/hour=%H")


def main():
    # Create a Spark configuration
    conf = SparkConf().setAppName("Save UUIDs to HDFS").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Generate 10 random UUIDs
    uuid_list = [str(uuid.uuid4()) for _ in range(10)]

    # Parallelize the UUID list to create an RDD
    uuid_rdd = sc.parallelize(uuid_list)

    # Get the current timestamp
    execution_time = int(datetime.now().timestamp() * 1000)
    time_partition = get_time_partition(execution_time)

    # Save the RDD as a text file to HDFS
    uuid_rdd.saveAsTextFile(f"hdfs://hadooop-hadoop-hdfs-nn:9000/spark-job/{time_partition}/{execution_time}")

    # Stop the Spark context
    sc.stop()


if __name__ == "__main__":
    main()