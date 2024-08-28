from transformations import *

def show_options(new):
    print("Choose the option you want to execute:")
    print("\t 0 - Exit")
    print("\t 1 - Convert estacio date to iso")
    print("\t 2 - Set Capacity")
    print("\t 3 - Add fields to capacity file")
    print("\t 4 - Create MongoDB")
    print("\t 5 - Verify MongoDB")


new = True
show_options(new)
op = int(input())

while op != 0:
    if op == 1:
        hdfs_path = "/formatted-zone/estat_estacio/"
        output_base_path="hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat_estacio"
        convert_informacio_estacio_dates(hdfs_path,output_base_path)
    elif op == 2:
        set_capacity(
            informacio_folder="hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/informacio",
            output_folder="hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/capacity_by_year"
        )
        print("capacity set!")
    elif op == 3:
        add_fields_to_capacity(
            capacity_path="hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/capacity_by_year",
            info_path_last_file="hdfs://hadooop-hadoop-hdfs-nn:9000/formatted-zone/informacio/year=2024/month=07",
            capacity_path_output="hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/capacity_by_year_all_fields"
        )
    elif op == 4:
        create_mongodb_bicing("hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/estat_estacio", "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/capacity_by_year_all_fields")        # Read estat_estacio Data
    elif op == 5:
        verify_mongo_writing()
    else:
        print ("Exiting ...")
        sys.exit()

    show_options(new)
    op = int(input())