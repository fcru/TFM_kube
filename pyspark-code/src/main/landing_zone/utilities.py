import os

import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import py7zr
from datetime import datetime
import io
from pyspark.sql import SparkSession


load_dotenv()

def get_time_partition(timestamp):
    # Convert the timestamp to a datetime object
    date = datetime.fromtimestamp(timestamp / 1000.0)

    # Format the datetime object into the desired format
    return date.strftime("year=%Y/month=%m/day=%d/hour=%H")

def scrape_website(URL) :

    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    script = soup.find_all("script")[1].text.strip()
    data = json.loads(script)
    return data

def call_get_API(content,TOKEN="", type_api=None, host = 'opendata-ajuntament.barcelona.cat'):
    session = requests.session()
    url = content['contentUrl']
    headers = {
        "Authorization": TOKEN,
        "Host": host
    }
    response = session.get(url, headers=headers)
    return response

    # Get the current timestamp

def save_hdfs_realtime():
    execution_time = int(datetime.now().timestamp() * 1000)
    time_partition = get_time_partition(execution_time)
    data = response.json()
    now = datetime.now().strftime("%y%m%d_%H%M")
    save_file = open(f"hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/stream/{time_partition}/{execution_time}/response_{type_api}.json", "w")
    json.dump(data, save_file, indent=6)
    save_file.close()


def save_7z(response):
    with open('temp.7z', 'wb') as f:
        f.write(response.content)

def save_hdfs_batch(spark, path, file_name):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.write.mode('overwrite').parquet(f"hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/{file_name}")
    print(f"File {file_name} saved in parquet")


def extract_csv_from_7z(seven_z_path):
    with py7zr.SevenZipFile(seven_z_path, mode='r') as z:
        file_name = z.getnames()[0]
        path = f"./tmp/{file_name}"# Get the name of the single file
        z.extractall(path=path)
        print (f"File {file_name} saved in tmp")
        return path, file_name



