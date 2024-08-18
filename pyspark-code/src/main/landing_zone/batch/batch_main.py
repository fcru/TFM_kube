from batch_scrape_bicing import batch_bicing_download
import sys


def show_options(new):
    print("Choose the option you want to execute:")
    print("\t 0 - Exit")
    print("\t 1 - Batch download Información Etación files")
    print("\t 2 - Batch download Estado Etación files")
    print("\t 3 - to do 3")
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
        print("to do ")
    elif op == 4:
        print("to do")

    else:
        print ("Exiting ...")
        sys.exit()

    show_options(new)
    op = int(input())