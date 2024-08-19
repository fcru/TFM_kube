import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import py7zr
from datetime import datetime
import os
from geojson import FeatureCollection


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


def call_get_API(url, host='opendata-ajuntament.barcelona.cat', TOKEN='', params={}):
    session = requests.session()
    headers = {
        "Authorization": TOKEN,
        "Host": host
    }
    try:
        response = session.get(url, headers=headers, params = params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} for {url}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err} for {url}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err} for {url}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err} for {url}")

        # If we reach here, an exception occurred
    return None

def save_json_hdfs(spark,response,file_name):
    data = response.json()
    json_str = json.dumps(data)
    rdd = spark.sparkContext.parallelize([json_str])
    df = spark.read.json(rdd)
    hdfs_path=f"hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/TMB/{file_name}"
    df.write.mode("overwrite").json(hdfs_path)
    print(f"JSON file successfully written to {hdfs_path}")

def from_hdfs_to_Geojson(spark,data):
    df = spark.read.json(data)
    geojson_data = df.toJSON().collect()
    # Create a list of features
    features = [json.loads(item) for item in geojson_data]
    # Assuming the original structure was a FeatureCollection
    geojson_feature_collection = FeatureCollection(features)
    # Print or use the GeoJSON data as needed
    print(json.dumps(geojson_feature_collection, indent=2))

def save_7z(response):
    with open('temp.7z', 'wb') as f:
        f.write(response.content)

def save_hdfs_batch(spark, path, file_name, hdfs_path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df.write.mode('overwrite').parquet(hdfs_path)
    print(f"File {file_name} saved in parquet")

def check_file_existence_hdfs(spark, hdfs_path):
    # Check if the path already exists
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI.create(hdfs_path),
                                                        spark._jsc.hadoopConfiguration(), )
    path_exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))
    return path_exists

def extract_csv_from_7z(seven_z_path,spark,type_api):
    with py7zr.SevenZipFile(seven_z_path, mode='r') as z:
        file_name = z.getnames()[0]
        path = f"./tmp/{file_name}"# Get the name of the single file
        hdfs_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/{type_api}/{file_name}"
        path_exists= check_file_existence_hdfs(spark, hdfs_path)
        if path_exists:
            print(f"Warning: {file_name} already exists.")
            print("Operation cancelled.")
            return
        z.extractall(path=path)
        print (f"File {file_name} saved in tmp")
        return path, file_name, hdfs_path

def cleanup_tmp_folder():
    tmp_folder = "./tmp"
    for filename in os.listdir(tmp_folder):
        file_path = os.path.join(tmp_folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
    print("Tmp folder cleaned up")



