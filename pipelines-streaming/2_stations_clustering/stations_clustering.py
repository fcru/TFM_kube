from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
import urllib.request
import json
import logging
from neo4j import GraphDatabase

# Neo4jConnection Class
class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result

uri = "neo4j://neo4j-neo4j:7687"

# Clean up neo4j database
neo4j_conn = Neo4jConnection(uri, "", "")
neo4j_conn.query("MATCH (n) DETACH DELETE n")


# Create a Spark session
spark = SparkSession.builder \
    .appName("Clustering") \
    .getOrCreate()

# Define the schema for the Informació Estacions DataFrame
schemaInfo = StructType([
    StructField("station_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("physical_configuration", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("altitude", FloatType(), True),
    StructField("address", StringType(), True),
    StructField("post_code", StringType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("is_charging_station", BooleanType(), True),
    StructField("short_name", IntegerType(), True),
    StructField("nearby_distance", FloatType(), True),
    StructField("_ride_code_support", BooleanType(), True),
    StructField("rental_uris", StringType(), True)  # Usamos StringType() para manejar valores null
])

# Function to fetch the Informació Estacions DataFrame
def fetch_data(url: str) -> DataFrame:
    try:
        # Download data using urllib
        request = urllib.request.Request(url)
        request.add_header('Authorization', 'b8092b37b02cda27d5d8e56cde9bfa9a49b15dab99bb06a06e89f72a931fa644')
        with urllib.request.urlopen(request) as response:
            data = response.read().decode('utf-8')

        # Convert JSON data to a Python object
        json_obj = json.loads(data)
        stations = json_obj["data"]["stations"]

        # Create a PySpark DataFrame from the JSON
        df = spark.createDataFrame(stations, schemaInfo)
        return df
    except Exception as e:
        logging.error(f"Error al descargar o procesar los datos: {e}")
        return None

# Call fetch_data
url = 'https://opendata-ajuntament.barcelona.cat/data/ca/dataset/informacio-estacions-bicing/resource/f60e9291-5aaa-417d-9b91-612a9de800aa/download/Informacio_Estacions_Bicing_securitzat.json'
dfInfo = fetch_data(url)

# Configure VectorAssembler for latitude and longitude
assembler = VectorAssembler(inputCols=["lat", "lon"], outputCol="features")
dfInfo_with_features = assembler.transform(dfInfo)

# Apply K-means
num_clusters = 25
kmeans = KMeans(k=num_clusters, seed=1, featuresCol="features", predictionCol="truck")
model = kmeans.fit(dfInfo_with_features)
dfInfo_with_clusters = model.transform(dfInfo_with_features)

# Write the nodes assigned to a cluster to Neo4j
dfInfo_with_clusters.select("station_id", "capacity", "truck", "lat", "lon").write \
    .format("org.neo4j.spark.DataSource") \
    .option("url", uri) \
    .mode("overwrite") \
    .option("labels", "Station") \
    .option("node.keys", "station_id") \
    .save()

# Create as many nodes as clusters created. Each node represents a truck

# Define the schema for the DataFrame
schema = StructType([
    StructField("truck_id", IntegerType(), True),
    StructField("capacity", IntegerType(), True)
])

# Create the data with dynamic truck_id and fixed capacity
capacity = 30
data = [(i, capacity) for i in range(0, num_clusters)]

# Write trucks df
trucks_df = spark.createDataFrame(data, schema)
trucks_df.write \
    .format("org.neo4j.spark.DataSource") \
    .option("url", uri) \
	.mode("overwrite") \
    .option("labels", "Truck") \
    .option("node.keys", "truck_id") \
	.save()

# Write relations between Stations and Trucks
cypher_query = """
MATCH (t:Truck), (s:Station)
WHERE t.truck_id = s.truck
CREATE (t)-[:SERVES]->(s)
"""

neo4j_conn.query(cypher_query)
neo4j_conn.close()
