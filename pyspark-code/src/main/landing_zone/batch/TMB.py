import sys
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

APP_ID = os.getenv('APP_ID')
APP_KEY = os.getenv('APP_KEY')

def download_TMB_data(url,file_name):
    spark = SparkSession.builder \
        .appName("Save file to hdfs") \
        .master("local[*]").getOrCreate()

    params = {
        'app_id': APP_ID,
        'app_key':APP_KEY
    }
    response = call_get_API(url, host='api.tmb.cat', params=params)
    save_json_hdfs(spark,response,file_name)