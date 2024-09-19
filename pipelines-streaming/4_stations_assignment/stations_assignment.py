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

# Write relationships with distance between stations of each cluster
insert_query = """
    MATCH (a:Station) <-[r1:SERVES]- (t:Truck) -[r2:SERVES]-> (b:Station)
    WHERE a.truck = b.truck
    AND a.station_id <> b.station_id
    AND a.check_status IN ['low', 'full']
    AND b.check_status IN ['low', 'full']
    WITH a, b,
         point({longitude: a.lon, latitude: a.lat}) AS pointA,
         point({longitude: b.lon, latitude: b.lat}) AS pointB
    WITH a, b, pointA, pointB,
         distance(pointA, pointB) AS distance
    MERGE (a)-[r:TO_INTERVENT {distance: distance}]->(b)
"""

delete_query = """
MATCH ()-[r:TO_INTERVENT]->()
DELETE r
"""

neo4j_conn = Neo4jConnection(uri, "", "")
neo4j_conn.query(delete_query)

neo4j_conn.query(insert_query)
neo4j_conn.close()
