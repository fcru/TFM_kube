import time

from batch_scrape_bicing import batch_bicing_download
from batch_api import *


if __name__ == "__main__":

    # Downloading Bicing Data

    print("Processing Information Bicing Stations...")
    url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/informacio-estacions-bicing"
    type_api="informacio"
    batch_bicing_download(url, type_api)
    print("Processed")
    time.sleep(5)

    print("Processing Status Bicing Stations...")
    url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/estat-estacions-bicing"
    type_api = "estat_estacio"
    batch_bicing_download(url, type_api)
    print("Processed")
    time.sleep(5)

    # Downloading TMB Data

    print("Processing Bus and metro lines...")
    url = ("https://api.tmb.cat/v1/transit/linies/bus")
    download_API_data(url,'bus_lines.json', "TMB")
    time.sleep(5)
    url = "https://api.tmb.cat/v1/transit/linies/metro"
    download_API_data(url, 'metro_lines.json', "TMB")
    time.sleep(5)
    url = "https://api.tmb.cat/v1/transit/estacions"
    download_API_data(url, 'metro_stations.json', "TMB")
    time.sleep(5)
    url = "https://api.tmb.cat/v1/transit/parades"
    download_API_data(url, 'bus_stops.json', "TMB")
    print("Processed")

    # Downloading BCN Data

    print("Processing Bcn data..")
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/"
           "resource/75197dfe-0306-4c5e-9643-34948af07fb6/download")
    download_API_data(url, 'bcn_neighbourhood.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/"
           "resource/db90a207-d125-4f80-aac5-f9d5d6e648f5/download")
    download_API_data(url, 'bcn_census_areas.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/e3497ea4-0bae-4093-94a7-119df50a8a74/"
           "resource/4608cf0c-2f11-4a25-891f-c5afc3af82c5/download")
    download_API_data(url, 'bcn_bike_lanes.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/5aeb5feb-29dd-4873-8b9a-21355fa90d59/"
           "resource/74cf2868-6bfd-4c59-8883-8c577a9b4df8/download")
    download_API_data(url, 'bcn_no_motorized_vehicles.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/462e7ea8-aa84-4892-b93f-3bc9ab8e5b4b/resource/0043bdda-0143-46c3-be64-d35cbc3a86f6/download")
    download_API_data(url, 'cultural_points_of_interests.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/5ce7863c-789a-46b5-977d-c5df4e263f94/resource/214ccb96-16d4-45aa-b0f5-cf8941f52400/download")
    download_API_data(url, 'Large_shopping_centers.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/62fb990e-4cc3-457a-aea1-497604e15659/resource/"
           "495c434e-b005-416e-b760-dc79f56dff3a/download/2019_censcomercialbcn_detall.geojson")
    download_API_data(url, 'commercial_census.json', "bcn_data")
    time.sleep(5)
    url = ("https://opendata-ajuntament.barcelona.cat/data/dataset/1e912e9e-0056-4f47-927e-9258419c9b74/resource/"
           "94c7ea5d-c0b3-4482-bea8-6d5023844798/download")
    download_API_data(url, 'educative_centers.json', "bcn_data")
    print("Processed")