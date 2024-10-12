import sys
import os
import gc
import time
import calendar
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession

#Para poder importar y utilizar todas las funciones definidas en el fichero de utilidades hay que indicarle el directorio donde se encuentra, 
# que es el anterior al de este fichero
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

load_dotenv()
API_KEY_METEOCAT = os.getenv('API_KEY_METEOCAT')

def download_Meteocat_data(url, variable, file, context, year):
    spark = SparkSession.builder \
        .appName("Save file to hdfs") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.cores", "6") \
        .master("local[*]").getOrCreate()
    
    # obtener datos de la variable especificada para todos los días del año entre 01/03/2019 y 
    # el dìa de ayer
    cal = calendar.Calendar()
    current_year = datetime.now().year
    current_month = 7 # datetime.now().month
    
    for month in range(1,13):
        if year == current_year and month > current_month:
            break

        for day in cal.itermonthdays(year, month):
            if day > 0:
                url_api = f"{url}/{variable}/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}"
                params = {
                    'codiEstacio': 'D5'
                }
                response = call_get_API(url_api, host='api.meteo.cat',TOKEN=API_KEY_METEOCAT,params=params)
            
                if response is None: break

                json_data = response.json()
                if len(json_data) == 0:
                    print(f"No data found for {year}-{month:02d}-{day:02d}, skipping.")
                    continue

                file_name = f"{file}_{year}_{str(month).zfill(2)}_{str(day).zfill(2)}.json"
                directorio = f"{context}/{file}"
                hdfs_path=f"hdfs://hadooop-hadoop-hdfs-nn:9000/landing-zone/batch/{directorio}/{file_name}"

                if check_file_existence_hdfs(spark,hdfs_path):
                    print(f"File: {file_name} is already saved")
                else:
                    save_json_hdfs(spark,response,file_name,directorio)
                
                #liberación de recursos
                del response
                gc.collect()

                spark._jvm.System.gc()

                time.sleep(1)
        
    spark.stop()

    spark._jvm.System.gc()