import sys
import os
import requests
import datetime
from dotenv import load_dotenv

#Para poder importar y utilizar todas las funciones definidas en el fichero de utilidades hay que indicarle el directorio donde se encuentra, 
# que es el anterior al de este fichero
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import *

load_dotenv()

def download_Meteocat_data(url, file_name):
    #llamar a la api por cada día del año entre el 01/01/2019 y hoy
    year = 2019
    
    while year <= datetime.date.today().year:
        month = 1
        while month <= 12:
            params = {
                'codiEstacio': 'D5',
                'any': year,
                'mes': str(month).zfill(2)
            }
            response = call_get_API(url, host='api.meteo.cat',TOKEN=API_KEY_METEOCAT,params=params)
            print(response.status_code)  #statusCode
            print(response.text) #valors de la resposta


 
#response = requests.get(url, headers={"Content-Type": "application/json", "X-Api-Key": key})
 
