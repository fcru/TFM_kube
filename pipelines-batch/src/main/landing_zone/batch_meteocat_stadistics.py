import sys
import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession

#Para poder importar y utilizar todas las funciones definidas en el fichero de utilidades hay que indicarle el directorio donde se encuentra, 
# que es el anterior al de este fichero
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

load_dotenv()
API_KEY_METEOCAT = os.getenv('API_KEY_METEOCAT')

def download_Meteocat_stadistics(url, file, context):
    spark = SparkSession.builder \
        .appName("Save file to hdfs") \
        .master("local[*]").getOrCreate()
    
    #llamar a la api por cada día del año entre el 01/01/2019 y hoy
    year = 2019
    current_year = datetime.now().year
    current_month = datetime.now().month
    while year <= current_year:
        month = 1
        while month <= 12:
            file_name = file + '_' + str(year) + '_' + str(month).zfill(2) + '.json'
            params = {
                'codiEstacio': 'D5',
                'any': year,
                'mes': str(month).zfill(2)
            }
            response = call_get_API(url, host='api.meteo.cat',TOKEN=API_KEY_METEOCAT,params=params)
            
            if response is None: break

            #print(response.status_code)  #statusCode
            #print(response.json()) #valors de la resposta

            save_json_hdfs(spark,response,file_name,context)

            month = month + 1
            if year == current_year:
                if month > current_month:
                    month = 13
        year = year + 1
    spark.stop()    
#response = requests.get(url, headers={"Content-Type": "application/json", "X-Api-Key": key})