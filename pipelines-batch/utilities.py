import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import py7zr
from datetime import datetime
import os


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


def call_get_API(url, host='opendata-ajuntament.barcelona.cat', TOKEN='', params={}, headers={}):
    session = requests.session()
    if TOKEN != '':
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

def save_json_hdfs(spark,response,file_name, context):
    data = response.json()
    json_str = json.dumps(data)
    rdd = spark.sparkContext.parallelize([json_str])
    df = spark.read.json(rdd)
    hdfs_path=f"hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/{context}/{file_name}"
    df.write.mode("overwrite").parquet(hdfs_path)
    print(f"JSON file successfully written to {hdfs_path}")


def save_7z(response):
    with open('temp.7z', 'wb') as f:
        f.write(response.content)

def save_csv_hdfs(spark, path, file_name, hdfs_path):
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


def list_files_by_condition (hdfs_path, fs, Path, condition='format'):
    file_status = fs.listStatus(Path(hdfs_path))
    files = []
    for status in file_status:
            path_dir = status.getPath().toString()
            if condition == 'format':
                if path_dir.endswith(".csv") or path_dir.endswith(".json"):
                  files.append(status.getPath().toString())
                else:
                  files.extend(list_files_by_condition(path_dir,fs,Path, condition))
            if condition == 'partition':
                if 'month=' in path_dir:
                  files.append(status.getPath().toString())
                else:
                  files.extend(list_files_by_condition(path_dir,fs,Path, condition))
    return files

def drop_existing_columns(columns_to_drop,df):

    # Filter the columns to drop based on their existence in the DataFrame
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]

    # Drop the existing columns
    df = df.drop(*existing_columns_to_drop)
    return df
