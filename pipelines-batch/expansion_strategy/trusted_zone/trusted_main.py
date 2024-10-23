import time

from format_bcn_data import *
from format_estado_Bicing import *
from format_info_Bicing import *
from formatted_functions import *
from missing_info_values import *
import time


if __name__ == "__main__":

    print("processing format TMB data...")
    output_base_TMB = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/TMB"
    hdfs_path = f"/landing-zone/batch/TMB/"
    format_bcn_data(output_base_TMB, hdfs_path)
    print("processed")
    time.sleep(5)

    print("processing format bcn data...")
    output_base_bcn = "hdfs://hadooop-hadoop-hdfs-nn:9000/trusted-zone/bcn_data"
    hdfs_path = f"/landing-zone/batch/bcn_data/"
    format_bcn_data(output_base_bcn, hdfs_path)
    print("processed")
    time.sleep(5)

    print("processing format bicing state...")
    formatBicingState()
    gc.collect()
    print("processed")
    time.sleep(5)

    print("processing date validation...")
    validar_ficheros_por_fecha()
    gc.collect()
    time.sleep(5)
    print("processed")

    print("processing station infos per day...")
    getInfoEstacionesXDay()
    print("processed")

    print("check missing values...")
    # Generate and print null values report
    print("converting in integer..")
    convert_to_integer_or_date()
    print("Converted!")
    null_values_count = generate_null_values_count_report()
    # Organize the data by station_id
    station_data = defaultdict(lambda: defaultdict(dict))
    for item in null_values_count:
        station_id = item['_id']['station_id']
        year = item['_id']['year']
        month = item['_id']['month']
        count = item['count']
        station_data[station_id][year][month] = count

    # Print the report
    print("Null Values Count Report:")
    print("-------------------------")
    for station_id, years in station_data.items():
        print(f"Station ID: {station_id}")
        for year, months in years.items():
            print(f"  Year {year}:")
            for month, count in months.items():
                print(f"    Month {month}: {count}")
        print()

    # Update documents with non-null values
    print("Updating documents with non-null values...")
    update_null_values()
    print("Documents updated successfully.")

    # Close the MongoDB connection
    client.close()

