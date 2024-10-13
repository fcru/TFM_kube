import sys
import os
from datetime import datetime, timedelta

from pymongo.errors import PyMongoError

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transformations import *

def show_options(new):
    print("Choose the option you want to execute:")
    print("\t 0 - Exit")
    print("\t 1 - Inspect files")
    print("\t 2 - Consolidate Informacio")
    print("\t 3 - Consolidate Estacio")
    print("\t 4 - Write geojson files to mongo")
    print("\t 5 - Write mongo info_estacio documents")


new = True
show_options(new)
op = int(input())

while op != 0:
    if op == 1:
        inspect_files()
    elif op == 2:
        consolidation_process()
    elif op == 3:
        consolidate_estacio()
    elif op == 4:
        tmb_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/TMB"
        print("Processing TMB directory..")
        process_json_files_to_mongodb(tmb_path)
        print("Processed")
        bcn_path = f"hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/bcn_data"
        union_df(f"{bcn_path}/bcn_bike_lanes.json", f"{bcn_path}/bcn_no_motorized_vehicles.json", f"{bcn_path}/bike_lanes_unified_no_motor.json")
        union_df(f"{bcn_path}/commercial_census.json", f"{bcn_path}/Large_shopping_centers.json",
                 f"{bcn_path}/all_commercials.json")
        print("Processing bcn_data directory..")
        process_json_files_to_mongodb(bcn_path)
        print("Processed")
    elif op == 5:
        write_informacio_to_mongo()
        print("Processed")
    else:
        print ("Exiting ...")
        sys.exit()

    show_options(new)
    op = int(input())