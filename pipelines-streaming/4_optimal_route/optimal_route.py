from kafka import KafkaProducer
from neo4j import GraphDatabase
from itertools import permutations
import json

# Neo4jConnection Class
class Neo4jConnection:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return list(result)

# URI for the Neo4j connection
uri = "neo4j://neo4j-neo4j:7687"

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    security_protocol='SASL_PLAINTEXT',  # Protocolo de seguridad
    sasl_mechanism='PLAIN',              # Mecanismo de SASL
    sasl_plain_username='user1',         # Usuario SASL (mismo que en jaas.conf)
    sasl_plain_password='LtG5496WgU',    # Contraseña SASL (mismo que en jaas.conf)
    key_serializer=lambda k: str(k).encode('utf-8'),  # Serializar la clave como string UTF-8
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializar el valor como JSON UTF-8
)

# Query to insert relationships with the distance between stations for each cluster
insert_query = """
    MATCH (a:Station) <-[r1:SERVES]- (t:Truck) -[r2:SERVES]-> (b:Station)
    WHERE a.truck = b.truck
        AND id(a) < id(b)
        AND a.check_status IN ['low', 'full']
        AND b.check_status IN ['low', 'full']
    WITH a, b,
         point({longitude: a.lon, latitude: a.lat}) AS pointA,
         point({longitude: b.lon, latitude: b.lat}) AS pointB
    WITH a, b, pointA, pointB,
         distance(pointA, pointB) AS distance
    MERGE (a)<-[r:TO_INTERVENT {distance: distance}]->(b)
"""

# Query to delete old relationships
delete_query = """
MATCH ()-[r:TO_INTERVENT]-()
DELETE r
"""

# Query to project the graph
project_graph_query = """
CALL gds.graph.project(
    'myGraph',
    'Station',
    {
        TO_INTERVENT: {
            orientation: 'UNDIRECTED',
            properties: ['distance']
        }
    }
)
"""

# Query to drop the graph
drop_graph_query = """
CALL gds.graph.drop('myGraph')
YIELD graphName
"""

# Function to calculate shortest path using Dijkstra's algorithm
def calculate_shortest_path(neo4j_conn, start_station_id, end_station_id):
    cypher_query = """
    MATCH (start:Station {station_id: $start_station_id})
    WITH id(start) AS startId
    MATCH (end:Station {station_id: $end_station_id})
    WITH startId, id(end) AS endId
    CALL gds.shortestPath.dijkstra.stream('myGraph', {
        sourceNode: startId,
        targetNode: endId,
        relationshipWeightProperty: 'distance'
    })
    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
    RETURN totalCost, nodeIds, costs
    """

    #print(f"Start Station ID: {start_station_id}, End Station ID: {end_station_id}")

    # Run the query with the provided station IDs
    result = neo4j_conn.query(cypher_query, parameters={
        "start_station_id": start_station_id,
        "end_station_id": end_station_id
    })

    total_cost = 0
    path = []
    for record in result:
        total_cost = record['totalCost']
        path = record['nodeIds']
        '''
        print(f"Total Distance: {total_cost}")
        print(f"Path Node IDs: {path}")
        print(f"Segment Costs: {record['costs']}")
        '''
    return total_cost, path

# Function to find the optimal route with a fixed start station
def find_optimal_route_fixed_start(neo4j_conn, start_station, station_list):
    stations = [start_station] + station_list
    n = len(stations)

    # Crear matriz de distancias
    distances = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            cost, _ = calculate_shortest_path(neo4j_conn, stations[i]['station_id'], stations[j]['station_id'])
            distances[i][j] = distances[j][i] = cost

    unvisited = set(range(1, n))  # Excluye la estación inicial
    current = 0  # Índice de la estación inicial
    path = [current]
    total_distance = 0
    balance_bicicletas = 0  # Inicia el balance

    # Incluir la estación inicial en el balance
    if stations[current]['check_status'] == 'low':
        balance_bicicletas += stations[current].get('bikes_to_refill', 0)
    elif stations[current]['check_status'] == 'full':
        balance_bicicletas -= stations[current].get('bikes_to_remove', 0)

    while unvisited:
        next_station = min(unvisited, key=lambda x: distances[current][x])
        unvisited.remove(next_station)
        path.append(next_station)
        total_distance += distances[current][next_station]
        current = next_station

        # Calcular balance para la estación actual
        if stations[current]['check_status'] == 'low':
            balance_bicicletas += stations[current].get('bikes_to_refill', 0)
        elif stations[current]['check_status'] == 'full':
            balance_bicicletas -= stations[current].get('bikes_to_remove', 0)

    # Regresar a la estación inicial
    path.append(0)
    total_distance += distances[current][0]

    # Convertir índices en IDs de estación
    optimal_path = [
        {
            "station_id": stations[i]['station_id'],
            "lat": stations[i]['lat'],
            "lon": stations[i]['lon'],
            "capacity": stations[i]['capacity'],
            "check_status": stations[i]['check_status'],
            "num_bikes_available": stations[i]['num_bikes_available'],
            "bikes_to_refill": stations[i].get('bikes_to_refill', 0),
            "bikes_to_remove": stations[i].get('bikes_to_remove', 0),
            "truck": stations[i]['truck']
        }
        for i in path
    ]

    return total_distance, optimal_path, balance_bicicletas

# Function to get stations connected by the TO_INTERVENT relationship for a specific truck
def get_stations_for_truck(neo4j_conn, truck_id):
    query = """
    MATCH (s:Station)-[:TO_INTERVENT]-(t:Station)
    WHERE s.truck = $truck_id OR t.truck = $truck_id
    RETURN DISTINCT
        s.station_id AS station_id,
        s.lat AS lat,
        s.lon AS lon,
        s.capacity AS capacity,
        s.check_status AS check_status,
        s.num_bikes_available AS num_bikes_available,
        s.bikes_to_refill AS bikes_to_refill,
        s.bikes_to_remove AS bikes_to_remove,
        s.truck AS truck
    """
    result = neo4j_conn.query(query, parameters={"truck_id": truck_id})
    return [
        {
            "station_id": record["station_id"],
            "lat": record["lat"],
            "lon": record["lon"],
            "capacity": record['capacity'],
            "check_status": record['check_status'],
            "num_bikes_available": record['num_bikes_available'],
            "bikes_to_refill": record.get('bikes_to_refill', 0),
            "bikes_to_remove": record.get('bikes_to_remove', 0),
            "truck": record['truck']
        }
        for record in result
    ]

# Function to get the list of trucks (clusters) from Neo4j
def get_trucks(neo4j_conn):
    result = neo4j_conn.query("MATCH (t:Truck) RETURN DISTINCT t.truck_id AS truck")
    return [record["truck"] for record in result]

# Send paths to Kafka with distance
def send_to_kafka(truck, optimal_route, distance, balance_bicicletas):
    message = {
        "truck_id": truck,
        "optimal_route": [
            {
                "station_id": station["station_id"],
                "lat": station["lat"],
                "lon": station["lon"],
                "capacity": station['capacity'],
                "check_status": station['check_status'],
                "num_bikes_available": station['num_bikes_available'],
                "bikes_to_refill": station.get('bikes_to_refill', 0),
                "bikes_to_remove": station.get('bikes_to_remove', 0),
                "truck": station['truck']
            } for station in optimal_route
        ],
        "distance": distance,
        "balance_bicicletas": balance_bicicletas
    }
    producer.send(topic="truck-route", key=truck, value=message)
    producer.flush()

# Main logic
def main():
    # Conectar a Neo4j
    neo4j_conn = Neo4jConnection(uri, "user", "password")

    try:
        neo4j_conn.query(delete_query)
        neo4j_conn.query(insert_query)
        neo4j_conn.query(project_graph_query)

        trucks = get_trucks(neo4j_conn)
        trucks.sort()

        # Calcular la ruta óptima para cada camión
        for truck in trucks:
            station_list = get_stations_for_truck(neo4j_conn, truck)

            if not station_list:
                print(f"No se encontraron estaciones para el camión {truck}")
                continue

            start_station = station_list[0]
            remaining_stations = station_list[1:]

            min_distance, optimal_route, balance_bicicletas = find_optimal_route_fixed_start(
                neo4j_conn, start_station, remaining_stations
            )

            print(f"Camión {truck} - Distancia Óptima: {min_distance} - Balance Bicicletas: {balance_bicicletas}")
            send_to_kafka(truck, optimal_route, min_distance, balance_bicicletas)

    finally:
        neo4j_conn.query(drop_graph_query)
        producer.close()
        neo4j_conn.close()

if __name__ == "__main__":
    main()