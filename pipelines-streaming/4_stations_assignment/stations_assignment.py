from neo4j import GraphDatabase
import itertools

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

    # Crear una matriz de distancias
    distances = [[0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i+1, n):
            cost, _ = calculate_shortest_path(neo4j_conn, stations[i], stations[j])
            distances[i][j] = distances[j][i] = cost

    # Implementar el algoritmo del vecino más cercano
    unvisited = set(range(1, n))  # Excluir la estación de inicio
    current = 0  # Índice de la estación de inicio
    path = [current]
    total_distance = 0

    while unvisited:
        next_station = min(unvisited, key=lambda x: distances[current][x])
        unvisited.remove(next_station)
        path.append(next_station)
        total_distance += distances[current][next_station]
        current = next_station

    # Volver a la estación de inicio
    path.append(0)
    total_distance += distances[current][0]

    # Convertir índices de vuelta a IDs de estación
    optimal_path = [stations[i] for i in path]

    return total_distance, optimal_path

# Function to get stations connected by the TO_INTERVENT relationship for a specific truck
def get_stations_for_truck(neo4j_conn, truck_id):
    query = """
    MATCH (s:Station)-[:TO_INTERVENT]-(t:Station)
    WHERE s.truck = $truck_id OR t.truck = $truck_id
    RETURN DISTINCT s.station_id AS station_id
    """
    result = neo4j_conn.query(query, parameters={"truck_id": truck_id})
    return [record["station_id"] for record in result]

# Function to get the list of trucks (clusters) from Neo4j
def get_trucks(neo4j_conn):
    result = neo4j_conn.query("MATCH (t:Truck) RETURN DISTINCT t.truck_id AS truck")
    return [record["truck"] for record in result]

# Main logic
def main():
    # Connect to Neo4j
    neo4j_conn = Neo4jConnection(uri, "", "")

    try:
        neo4j_conn.query(delete_query)
        neo4j_conn.query(insert_query)
        neo4j_conn.query(project_graph_query)

        trucks = get_trucks(neo4j_conn)
        trucks.sort()

        # Print the list of trucks obtained
        print(f"Trucks retrieved: {trucks}")

        # Calculate the optimal route for each truck
        for truck in trucks:
            station_list = get_stations_for_truck(neo4j_conn, truck)

            if not station_list:
                print(f"No stations found for truck {truck}")
                continue

            start_station = station_list[0]
            remaining_stations = station_list[1:]

            min_distance, optimal_route = find_optimal_route_fixed_start(neo4j_conn, start_station, remaining_stations)

            #print(f"Truck {truck} - Optimal Distance: {min_distance}")
            print(f"Truck {truck} - Optimal Route Path: {optimal_route}")

    finally:
        neo4j_conn.query(drop_graph_query)
        neo4j_conn.close()

if __name__ == "__main__":
    main()