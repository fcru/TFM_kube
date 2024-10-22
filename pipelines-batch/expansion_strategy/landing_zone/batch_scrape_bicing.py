import sys
import os
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *


def batch_bicing_download(url, type_api):
    spark = SparkSession.builder \
        .appName("Save file to hdfs") \
        .master("local[*]").getOrCreate()

    data = scrape_website(url)
    content_data = data['distribution'][1:]
    #print(content_data)
    for content in content_data:
        url = content['contentUrl']
        response = call_get_API(url)
        if response is not None:
            save_7z(response)
            result = extract_csv_from_7z("temp.7z",spark,type_api)
            if result:
                path, file_name, hdfs_path = result
                save_csv_hdfs(spark, path, file_name, hdfs_path)
                cleanup_tmp_folder()

    spark.stop()



