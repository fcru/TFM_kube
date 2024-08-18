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
        response = call_get_API(content)
        if response is not None:
            save_7z(response)
            result = extract_csv_from_7z("temp.7z",spark,type_api)
            if result:
                path, file_name = result
                save_hdfs_batch(spark, path, file_name,type_api)
                cleanup_tmp_folder()

    spark.stop()



