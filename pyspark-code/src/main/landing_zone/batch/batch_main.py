from Cryptodome.SelfTest.Cipher.test_OFB import file_name

from batch_scrape_bicing import batch_bicing_download
from TMB import *
import sys



def show_options(new):
    print("Choose the option you want to execute:")
    print("\t 0 - Exit")
    print("\t 1 - Batch download Información Etación files")
    print("\t 2 - Batch download Estado Etación files")
    print("\t 3 - Batch download TMB data(bus and metro lines, bus stops and metro stations  ")
    print("\t 4 - to do 4")

new = True
show_options(new)
op = int(input())

while op != 0:
    if op == 1:
        url = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/informacio-estacions-bicing"
        type_api="informacio"
        batch_bicing_download(url, type_api)
    elif op == 2:
        URL = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/estat-estacions-bicing"
        type_api = "estat_estacio"
        batch_bicing_download(url, type_api)
    elif op == 3:
        url = "https://api.tmb.cat/v1/transit/linies/bus"
        download_TMB_data(url,'bus_lines')

        url = "https://api.tmb.cat/v1/transit/linies/metro"
        download_TMB_data(url, 'metro_lines')

        url = "https://api.tmb.cat/v1/transit/estacions"
        download_TMB_data(url, 'metro_stations')

        url = "https://api.tmb.cat/v1/transit/parades"
        download_TMB_data(url, 'bus_stops')


    elif op == 4:
        print("to do")

    else:
        print ("Exiting ...")
        sys.exit()

    show_options(new)
    op = int(input())