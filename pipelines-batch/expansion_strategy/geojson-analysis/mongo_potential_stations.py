import numpy as np
import geopandas as gpd
from scipy.spatial import cKDTree
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.cluster import DBSCAN
from shapely.geometry import Point as sh_point
from scipy.interpolate import LinearNDInterpolator
from pymongo import MongoClient
from bson.json_util import dumps
from utilities import *

def nearest_dist(gdf1, gdf2):
    coords1 = np.array(list(gdf1.geometry.apply(lambda x: (x.x, x.y))))
    coords2 = np.array(list(gdf2.geometry.apply(lambda x: (x.x, x.y))))
    tree = cKDTree(coords2)
    distances, _ = tree.query(coords1)
    return distances


def interpolate_altitude(known_points, known_altitudes, interpolation_points):
    interpolator = LinearNDInterpolator(known_points, known_altitudes)
    return interpolator(interpolation_points)


def optimize_dbscan_params(data, min_eps, max_eps, eps_step, min_samples_range):
    best_score = -np.inf
    best_eps = None
    best_min_samples = None

    for eps in np.arange(min_eps, max_eps, eps_step):
        for min_samples in min_samples_range:
            db = DBSCAN(eps=eps, min_samples=min_samples).fit(data)
            n_clusters = len(set(db.labels_)) - (1 if -1 in db.labels_ else 0)
            n_noise = list(db.labels_).count(-1)

            score = n_clusters - (n_noise / len(data))

            if score > best_score:
                best_score = score
                best_eps = eps
                best_min_samples = min_samples

    return best_eps, best_min_samples, best_score

def convert_to_geojson(item):
    if 'geometry' in item and hasattr(item['geometry'], '__geo_interface__'):
        item['geometry'] = mapping(item['geometry'])
    return item


def save_stations_to_mongodb(new_stations):
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['bicing_db']
    collection_proposed = db['proposed_station']

    # Check the type of new_stations and process accordingly
    if isinstance(new_stations, dict):
        # If it's already a dictionary, we'll assume it's a single station
        data_to_insert = [convert_to_geojson(new_stations)]
        print("It's a dictionary")
    elif hasattr(new_stations, 'to_crs'):
        # If it's a GeoDataFrame, convert to EPSG:4326 and then to a list of dicts
        new_stations = new_stations.to_crs(4326)
        data_to_insert = [convert_to_geojson(item) for item in new_stations.to_dict(orient='records')]
        print("It's a GeoDataFrame")
    elif hasattr(new_stations, 'to_dict'):
        # If it's a regular DataFrame, convert to a list of dicts
        data_to_insert = [convert_to_geojson(item) for item in new_stations.to_dict(orient='records')]
        print("It's a DataFrame")
    else:
        raise ValueError("new_stations must be a DataFrame, GeoDataFrame, or dictionary")

    collection_proposed.drop()

    inserted_count = 0
    for document in data_to_insert:
        # Remove the _id field if it exists
        document.pop('_id', None)

        # Use update_one with upsert=True
        result = collection_proposed.update_one(
            {'geometry': document['geometry']},  # Use geometry as a unique identifier
            {'$set': document},
            upsert=True
        )

        if result.upserted_id:
            inserted_count += 1
        elif result.modified_count:
            inserted_count += 1

    print(f"Inserted or updated {inserted_count} stations in MongoDB")

    client.close()


def update_top_stations_with_distances(gdf_top_stations):

    # Connect to MongoDB
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['bicing_db']
    collection = db['top_stations']

    # Update each document in the collection
    for index, row in gdf_top_stations.iterrows():
        station_id = row['station_id']
        update_data = {
            'dist_education': float(row['dist_education']),
            'dist_shopping': float(row['dist_shopping']),
            'dist_cpoi': float(row['dist_cpoi']),
            'dist_bus_stops': float(row['dist_bus_stops']),
            'dist_metro_stations': float(row['dist_metro_stations']),
            'dist_bike_lanes': float(row['dist_bike_lanes']),
            'dist_popular': float(row['dist_popular']),
            'dist_bike_station': float(row['dist_bike_station'])
        }

        collection.update_one({'station_id': station_id}, {'$set': update_data})

    print(f"Updated {len(gdf_top_stations)} documents in the top_stations collection")

    client.close()

def get_similar_stations(projected=True):
    # Load data
    gdf_top_stations, top_stations_error = doc_to_gdf('top_stations',
                                                      fields=['name', 'station_id', 'geometry', 'reason'])
    gdf_no_bikes = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'No Bikes' in x)]
    gdf_no_docks = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'No Docks' in x)]
    gdf_high = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'High Rotation' in x)]
    gdf_stations, stations_error = filter_estacio_to_gdf(gdf_top_stations)
    if gdf_stations is None or gdf_top_stations is None:
        print("Failed to load station data. Please check your database connection and data integrity.")
        if stations_error:
            print(f"Error loading stations: {stations_error}")
        if top_stations_error:
            print(f"Error loading stations: {top_stations_error}")
        return None

    # Project CRS to Barcelona
    if projected:
        crs = 'EPSG:25831'
        gdf_stations = gdf_stations.to_crs(crs)
        gdf_top_stations = gdf_top_stations.to_crs(crs)
        step_size = 10
        min_distance_st = 200
        max_distance_bike= 60
    else:
        step_size = 0.002
        crs = "EPSG:4326"
        min_distance = 0.02
        eps = 0.001

    # Load other layers
    other_layers = {}
    layer_configs = {
        "Bike Lanes": ('bike_lanes_unified_no_motor'),
        "Neighbourhoods": ('bcn_neighbourhood'),
        "Bus Stops": ('bus_stops'),
        "Commercial Census": ('all_commercials'),
        "Cultural Points of Interest": ('cultural_points_of_interests'),
        "Educational Centers": ('educative_centers'),
        "Metro Stations": ('metro_stations')
    }

    for layer_name, collection_name in layer_configs.items():
        gdf, error = doc_to_gdf(collection_name)
        if gdf is not None:
            gdf = gdf.to_crs(crs)
            other_layers[layer_name] = gdf

    # Calculate features for top stations
    gdf_top_stations['dist_education'] = nearest_dist(gdf_top_stations, other_layers['Educational Centers'])
    gdf_top_stations['dist_shopping'] = nearest_dist(gdf_top_stations, other_layers['Commercial Census'])
    gdf_top_stations['dist_cpoi'] = nearest_dist(gdf_top_stations, other_layers['Cultural Points of Interest'])
    gdf_top_stations['dist_bus_stops'] = nearest_dist(gdf_top_stations, other_layers['Bus Stops'])
    gdf_top_stations['dist_metro_stations'] = nearest_dist(gdf_top_stations, other_layers['Metro Stations'])
    gdf_top_stations['dist_bike_lanes'] = gdf_top_stations.distance(other_layers["Bike Lanes"].unary_union)
    gdf_top_stations['dist_popular'] = nearest_dist(gdf_top_stations, gdf_top_stations)
    gdf_top_stations['dist_bike_station'] = nearest_dist(gdf_top_stations, gdf_stations)

    update_top_stations_with_distances(gdf_top_stations)
    print("Top Stations")
    print(gdf_top_stations.describe())
    # Create a grid of potential locations
    xmin, ymin, xmax, ymax = other_layers['Neighbourhoods'].total_bounds
    x = np.arange(xmin, xmax, step_size)
    y = np.arange(ymin, ymax, step_size)
    xx, yy = np.meshgrid(x, y)
    potential_points_coords = np.column_stack((xx.ravel(), yy.ravel()))

    #interpolated_altitudes = interpolate_altitude(known_points, known_altitudes, potential_points_coords)

    # Create potential points with interpolated altitudes
    points = [sh_point(x, y) for x, y in zip(xx.ravel(), yy.ravel())]
    potential_points = gpd.GeoDataFrame(geometry=points, crs=crs)
    #potential_points['altitude'] = interpolated_altitudes

    # Perform spatial join with neighborhoods
    neighborhoods = other_layers['Neighbourhoods']
    potential_points_within = gpd.sjoin(potential_points, neighborhoods, how="inner", op="within")

    # Calculate features for potential points
    potential_points_within['dist_education'] = nearest_dist(potential_points_within, other_layers['Educational Centers'])
    potential_points_within['dist_shopping'] = nearest_dist(potential_points_within, other_layers['Commercial Census'])
    potential_points_within['dist_cpoi'] = nearest_dist(potential_points_within, other_layers['Cultural Points of Interest'])
    potential_points_within['dist_bus_stops'] = nearest_dist(potential_points_within, other_layers['Bus Stops'])
    potential_points_within['dist_metro_stations'] = nearest_dist(potential_points_within, other_layers['Metro Stations'])
    potential_points_within['dist_bike_lanes'] = potential_points_within.distance(other_layers["Bike Lanes"].unary_union)
    potential_points_within['dist_popular'] = nearest_dist(potential_points_within, gdf_top_stations)
    potential_points_within['dist_bike_station'] = nearest_dist(potential_points_within, gdf_stations)

    # Define features
    features = ['dist_education', 'dist_shopping', 'dist_bike_lanes', 'dist_cpoi', 'dist_bus_stops',
                'dist_metro_stations', 'dist_popular', 'dist_bike_station']

    # Filter out points too close to existing popular stations
    potential_points_within = potential_points_within[potential_points_within['dist_popular'] > min_distance_st]
    potential_points_within = potential_points_within[potential_points_within['dist_bike_station'] > min_distance_st]
    potential_points_within = potential_points_within[potential_points_within['dist_bike_lanes'] < max_distance_bike]


    # Normalize features
    scaler = StandardScaler()
    gdf_top_stations_scaled = gdf_top_stations.copy()
    potential_points_scaled = potential_points_within.copy()

    gdf_top_stations_scaled[features] = scaler.fit_transform(gdf_top_stations[features])
    potential_points_scaled[features] = scaler.transform(potential_points_within[features])

    # Find similar environments
    n_neighbors = 5
    nn = NearestNeighbors(n_neighbors=n_neighbors, metric='euclidean')
    nn.fit(gdf_top_stations_scaled[features])

    distances, indices = nn.kneighbors(potential_points_scaled[features])

    # Calculate similarity score
    potential_points_within['similarity_score'] = 1 / distances.mean(axis=1)

    # Select top scoring points
    top_points = potential_points_within.sort_values('similarity_score', ascending=False).head(100)

    # Optimize DBSCAN parameters
    coords = top_points.geometry.apply(lambda geom: (geom.x, geom.y)).tolist()
    if projected:
        min_eps, max_eps, eps_step = 50, 300, 50
    else:
        min_eps, max_eps, eps_step = 0.0005, 0.002, 0.0001

    best_eps, best_min_samples, _ = optimize_dbscan_params(coords, min_eps, max_eps, eps_step, range(2, 6))
    print(f"best params are eps: {best_eps} samples: {best_min_samples}")

    # Use optimized parameters for DBSCAN
    db = DBSCAN(eps=best_eps, min_samples=best_min_samples).fit(coords)
    top_points['cluster'] = db.labels_

    # Filter out noise points (-1 cluster) from top_points
    top_points_filtered = top_points[top_points['cluster'] != -1]

    print("Top Points without cluster -1")
    print(top_points_filtered.describe())

    # Select the highest scoring point from each cluster
    new_stations = top_points_filtered.loc[top_points_filtered.groupby('cluster')['similarity_score'].idxmax()]

    # Change CRS only for the geometry column
    new_stations = new_stations.copy()
    new_stations['geometry'] = new_stations['geometry'].to_crs("EPSG:4326")

    print(f"Number of proposed new stations: {len(new_stations)}")
    print("\nTop 5 proposed locations:")
    print(new_stations.head())

    # Save to MongoDB if connection details are provided
    save_stations_to_mongodb(new_stations)

    return new_stations


if __name__ == "__main__":
    get_similar_stations()