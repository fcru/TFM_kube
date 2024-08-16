import sys
import os

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities import *

spark = SparkSession.builder \
        .appName("Save file to hdfs") \
        .master("local[*]").getOrCreate()

URL_estacio = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/estat-estacions-bicing"
URL_informacio = "https://opendata-ajuntament.barcelona.cat/data/es/dataset/informacio-estacions-bicing"


data = scrape_website(URL_informacio)
content_data = data['distribution'][1:]
print(content_data)



for content in content_data:
    type = content['encodingFormat']
    response = call_get_API(content)
    if type == 'JSON':
        print("to do")
    else:
        save_7z(response)
        path, file_name = extract_csv_from_7z("temp.7z")
        save_hdfs_batch(spark, path, file_name)


spark.stop()


