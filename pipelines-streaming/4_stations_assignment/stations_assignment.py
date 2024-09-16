from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp
from pyspark.sql.types import IntegerType
from neo4j import GraphDatabase
import itertools

# Clean up Station_to_Intervent nodes
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

neo4j_conn = Neo4jConnection(uri, "", "")
neo4j_conn.query("MATCH (n:Station_to_Intervent) DETACH DELETE n")
neo4j_conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("StationsAssignment") \
    .getOrCreate()

# Read data from Neo4j
df_with_status = spark.read \
    .format("org.neo4j.spark.DataSource") \
    .option("url", "neo4j://neo4j-neo4j:7687") \
    .option("labels", "Station") \
    .load()

# Filter stations based on 'low' and 'full' status
filtered_stations_df = df_with_status \
    .filter((col("check_status") == "low") | (col("check_status") == "full"))

# Calculate bikes_to_refill for "low" stations and bikes_to_remove for "full" stations in a single step
filtered_stations_df = filtered_stations_df.withColumns({
    "bikes_to_refill": when(col("check_status") == "low",
                            (col("capacity") - (col("capacity") * 0.25) - col("num_bikes_available")).cast(IntegerType())
                           ).otherwise(None),
    "bikes_to_remove": when(col("check_status") == "full",
                            (col("capacity") * 0.25).cast(IntegerType())
                           ).otherwise(None)
})

# Prepare data to write back to Neo4j, adding the `lastUpdate` column
to_neo4j = filtered_stations_df \
    .withColumn("lastUpdate", current_timestamp())

# Stream to write nodes to Neo4j and execute the Cypher query after each batch
to_neo4j.write \
    .format("org.neo4j.spark.DataSource") \
    .option("url", uri) \
	.mode("overwrite") \
    .option("labels", "Station_to_Intervent") \
    .option("node.keys", "station_id") \
	.save()

# Write relationships with distance between stations of each cluster
cypher_query = """
    MATCH (a:Station_to_Intervent), (b:Station_to_Intervent)
    WHERE a.truck = b.truck AND a.station_id < b.station_id
    WITH a, b,
         point({longitude: a.lon, latitude: a.lat}) AS pointA,
         point({longitude: b.lon, latitude: b.lat}) AS pointB
    WITH a, b, pointA, pointB,
         distance(pointA, pointB) AS distance
    MERGE (a)-[r:SAME_TRUCK {distance: distance}]->(b)
"""
neo4j_conn = Neo4jConnection(uri, "", "")
neo4j_conn.query(cypher_query)
neo4j_conn.close()

# Function to calculate the shortest path between multiple stations, keeping the first station fixed
def find_optimal_route_fixed_start(neo4j_conn, start_station, station_list):
    min_distance = float('inf')
    optimal_path = None

    # Generate all possible permutations of the station list (except the start station)
    for perm in itertools.permutations(station_list):
        # Add the start station at the beginning of the route
        full_route = [start_station] + list(perm)
        # Calculate the total distance and path for this route
        total_distance, route_path = calculate_route_through_stations(neo4j_conn, full_route)
        # Update the optimal path if the current one is shorter
        if total_distance < min_distance:
            min_distance = total_distance
            optimal_path = route_path

    return min_distance, optimal_path

# Retrieve the list of station IDs from the Neo4j database and collect them into a list
station_list = to_neo4j.select("station_id").rdd.flatMap(lambda x: x).collect()

# Store the first station ID in a variable called start_station
start_station = station_list[0]

# Remove the first station ID from the list
station_list = station_list[1:]

# Establish Neo4j connection
neo4j_conn = Neo4jConnection(uri, "", "")

# Call the function to calculate the optimal route with a fixed start
min_distance, optimal_route = find_optimal_route_fixed_start(neo4j_conn, start_station, station_list)

# Close the Neo4j connection
neo4j_conn.close()