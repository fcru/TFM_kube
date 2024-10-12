from batch_scrape_bicing import batch_bicing_download
from batch_api import *
from batch_meteocat_stadistics import download_Meteocat_stadistics
from batch_meteocat_data import download_Meteocat_data
import sys

def show_options(new):
    print("Choose the option you want to execute:")
    print("\t 0 - Exit")
    print("\t 1 - Batch download Información Estación files")
    print("\t 2 - Batch download Estado Estación files")
    print("\t 3 - Batch download TMB data(bus and metro lines, bus stops and metro stations  ")
    print("\t 4 - Batch Download Barcelona districts and streets Geojson")
    print("\t 5 - Batch Download Barcelona points of interest")
    print("\t 6 - Batch Download Barcelona population by age and sex")
    print("\t 7 - Batch Download Meteocat statistics: mean by day (Temperatura, precipitación, humedad y viento)")
    print("\t 8 - Batch Download Meteocat diary data variables (temperatura, precipitación, humedad y viento)")
    print("\t 9 - Batch Download Barcelona Holidays")

new = True
show_options(new)
op = int(input())

while op != 0:
    if op == 1:
        url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/informacio-estacions-bicing"
        type_api="informacio"
        batch_bicing_download(url, type_api)
    elif op == 2:
        url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/estat-estacions-bicing"
        type_api = "estat_estacio"
        batch_bicing_download(url, type_api)
    elif op == 3:
        url = "https://api.tmb.cat/v1/transit/linies/bus"
        download_API_data(url,'bus_lines.json', "TMB")

        url = "https://api.tmb.cat/v1/transit/linies/metro"
        download_API_data(url, 'metro_lines.json', "TMB")

        url = "https://api.tmb.cat/v1/transit/estacions"
        download_API_data(url, 'metro_stations.json', "TMB")

        url = "https://api.tmb.cat/v1/transit/parades"
        download_API_data(url, 'bus_stops.json', "TMB")
    elif op == 4:
        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/"
               "resource/75197dfe-0306-4c5e-9643-34948af07fb6/download")
        download_API_data(url, 'bcn_neighbourhood.json', "bcn_data")

        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/e3497ea4-0bae-4093-94a7-119df50a8a74/"
               "resource/4608cf0c-2f11-4a25-891f-c5afc3af82c5/download")
        download_API_data(url, 'bcn_bike_lanes.json', "bcn_data")

        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/5aeb5feb-29dd-4873-8b9a-21355fa90d59/"
               "resource/74cf2868-6bfd-4c59-8883-8c577a9b4df8/download")
        download_API_data(url, 'bcn_no_motorized_vehicles.json', "bcn_data")

    elif op == 5:
        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/462e7ea8-aa84-4892-b93f-3bc9ab8e5b4b/resource/0043bdda-0143-46c3-be64-d35cbc3a86f6/download")
        download_API_data(url, 'cultural_points_of_interests.json', "bcn_data")

        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/5ce7863c-789a-46b5-977d-c5df4e263f94/resource/214ccb96-16d4-45aa-b0f5-cf8941f52400/download")
        download_API_data(url, 'Large_shopping_centers.json', "bcn_data")

        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/62fb990e-4cc3-457a-aea1-497604e15659/resource/"
               "495c434e-b005-416e-b760-dc79f56dff3a/download/2019_censcomercialbcn_detall.geojson")
        download_API_data(url, 'commercial_census.json', "bcn_data")

        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/1e912e9e-0056-4f47-927e-9258419c9b74/resource/"
               "94c7ea5d-c0b3-4482-bea8-6d5023844798/download")
        download_API_data(url, 'educative_centers.json', "bcn_data")

    elif op == 6:
        url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/76835d18-34b4-475f-8ea0-abdfb77d2c0a/resource/"
               "b703115b-6930-4e71-bbb3-0e507ebd252e/download")
        download_API_data(url, 'population_by_sex_age_2024.json', "bcn_data")
    elif op == 7:
        #temperatura media
        url = 'https://api.meteo.cat/xema/v1/variables/estadistics/diaris/1003'
        download_Meteocat_stadistics(url, 'temperatura', 'METEOCAT')

        #precipitación media
        url = 'https://api.meteo.cat/xema/v1/variables/estadistics/diaris/1300'
        download_Meteocat_stadistics(url, 'precipitacion', 'METEOCAT')

        #humedad media
        url = 'https://api.meteo.cat/xema/v1/variables/estadistics/diaris/1100'
        download_Meteocat_stadistics(url, 'humedad', 'METEOCAT')

        #viento media
        url = 'https://api.meteo.cat/xema/v1/variables/estadistics/diaris/1503'
        download_Meteocat_stadistics(url, 'viento', 'METEOCAT')
    elif op == 8:
        #url para la descarga de las lecturas periódicas de las variables de Meteocat
        url = 'https://api.meteo.cat/xema/v1/variables/mesurades'
        
        year = 2021
        current_year = datetime.now().year
        while year <= current_year:
            #Lecturas de Temperatura
            download_Meteocat_data(url, 32, 'temperatura', 'METEOCAT', year)
            #Lecturas de Precipitación
            download_Meteocat_data(url, 35, 'precipitacion', 'METEOCAT', year)
            #Lecturas de Humedad
            download_Meteocat_data(url, 33, 'humedad', 'METEOCAT', year)
            #Lecturas de Viento :falta viento del 22
            download_Meteocat_data(url, 30, 'viento', 'METEOCAT', year)

            year = year + 1
        #download_Meteocat_data(url, 30, 'viento', 'METEOCAT', 2022)
        #download_Meteocat_data(url, 30, 'viento', 'METEOCAT', 2023)
    elif op == 9:
        #Festivos generales de Catalunya
        url ='https://analisi.transparenciacatalunya.cat/resource/8qnu-agns.json'
        download_API_data(url, 'festivos_generales.json', "festivos")
        #Festivos locales de Catalunya
        url='https://analisi.transparenciacatalunya.cat/resource/b4eh-r8up.json?$limit=200000'
        download_API_data(url, 'festivos_locales.json', "festivos")
    else:
        print ("Exiting ...")
        sys.exit()

    show_options(new)
    op = int(input())