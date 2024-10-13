import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point as sh_point
from sklearn.cluster import DBSCAN
from scipy.spatial import cKDTree
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from utilities import *


# Function to calculate distance to nearest point
def nearest_dist(gdf1, gdf2):
    # Extract coordinates from the geometry column
    coords1 = np.array(list(gdf1.geometry.apply(lambda x: (x.x, x.y))))
    coords2 = np.array(list(gdf2.geometry.apply(lambda x: (x.x, x.y))))
    tree = cKDTree(coords2)
    distances, _ = tree.query(coords1)
    return distances

def get_similar_stations(projected = True):
    # Load data
    gdf_top_stations, top_stations_error = doc_to_gdf('top_stations', fields=['name', 'station_id', 'geometry', 'reason'])
    gdf_no_bikes = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'No Bikes' in x)]
    gdf_no_docks = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'No Docks' in x)]
    gdf_high = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'High Rotation' in x)]
    # Load station data with limited fields
    gdf_stations, stations_error = filter_estacio_to_gdf(gdf_top_stations)
    # gdf_popular_stations = getOverallPopularStations()
    # gdf_popular_stations, popular_stations_error = doc_to_gdf('popular_stations', limit=600, fields=['name', 'station_id', 'geometry'])
    if gdf_stations is None or gdf_top_stations is None:
        print("Failed to load station data. Please check your database connection and data integrity.")
        if stations_error:
            print(f"Error loading stations: {stations_error}")
        if top_stations_error:
            print(f"Error loading stations: {top_stations_error}")

    #project crs to barcelona
    if projected == True:
        crs = 'EPSG:25831'
        gdf_stations = gdf_stations.to_crs(crs)
        gdf_top_stations = gdf_top_stations.to_crs(crs)
        step_size=10
        min_distance = 50
        eps = 50
    else:
        step_size = 0.002
        crs = "EPSG:4326"
        min_distance = 0.02  # change to 200 if projected
        eps = 0.001


    # Load other layers with limits
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


    for layer_name, (collection_name) in layer_configs.items():
        gdf, error = doc_to_gdf(collection_name)
        if gdf is not None:
            gdf = gdf.to_crs(crs)
            other_layers[layer_name] = gdf


    # Calculate features for popular stations
    gdf_top_stations['dist_education'] = nearest_dist(gdf_top_stations, other_layers['Educational Centers'])
    gdf_top_stations['dist_shopping'] = nearest_dist( gdf_top_stations, other_layers['Commercial Census'])
    gdf_top_stations['dist_cpoi'] = nearest_dist(gdf_top_stations, other_layers['Cultural Points of Interest'])
    gdf_top_stations['dist_bus_stops'] =  nearest_dist(gdf_top_stations, other_layers['Bus Stops'])
    gdf_top_stations['dist_metro_stations'] = nearest_dist(gdf_top_stations, other_layers['Metro Stations'])
    gdf_top_stations['dist_bike_lanes'] = gdf_top_stations.distance(other_layers["Bike Lanes"].unary_union)

    # Add altitude data (you'll need to get this data from a DEM or other source)
     #popular_stations['altitude'] = get_altitude(popular_stations)

    # Calculate distance to nearest popular station
    gdf_top_stations['dist_popular'] = nearest_dist(gdf_top_stations, gdf_top_stations)
    gdf_top_stations['dist_bike_station'] = nearest_dist(gdf_top_stations, gdf_stations)

    # Normalize features
    scaler = StandardScaler()

    features = ['dist_education', 'dist_shopping', 'dist_bike_lanes', 'dist_cpoi', 'dist_bus_stops','dist_metro_stations','dist_popular','dist_bike_station']
    print("Popular Stations Analysis:")
    print(gdf_top_stations[features].describe())

    gdf_top_stations[features] = scaler.fit_transform(gdf_top_stations[features])
    # Analyze popular stations


    # Create a grid of potential locations
    xmin, ymin, xmax, ymax = other_layers['Neighbourhoods'].total_bounds
    x = np.arange(xmin, xmax, step_size)  # change to 20-meter step size if projected
    y = np.arange(ymin, ymax, step_size)
    xx, yy = np.meshgrid(x, y)

    # Create potential points correctly
    points = [sh_point(x, y) for x, y in zip(xx.ravel(), yy.ravel())]
    #potential_points = gpd.GeoDataFrame(geometry=points, crs=projected_crs)
    potential_points = gpd.GeoDataFrame(geometry=points, crs=crs)


    # Calculate features for potential points
    potential_points['dist_education'] = nearest_dist(potential_points, other_layers['Educational Centers'])
    potential_points['dist_shopping'] = nearest_dist(potential_points, other_layers['Commercial Census'])
    potential_points['dist_cpoi'] = nearest_dist(potential_points, other_layers['Cultural Points of Interest'])
    potential_points['dist_bus_stops'] = nearest_dist(potential_points, other_layers['Bus Stops'])
    potential_points['dist_metro_stations'] = nearest_dist(potential_points, other_layers['Metro Stations'])
    potential_points['dist_bike_lanes'] = potential_points.distance(other_layers["Bike Lanes"].unary_union)

    # Add altitude data (you'll need to get this data from a DEM or other source)
    # popular_stations['altitude'] = get_altitude(popular_stations)

    # Calculate distance to nearest popular station
    potential_points['dist_popular'] = nearest_dist(potential_points, gdf_top_stations)
    potential_points['dist_bike_station'] = nearest_dist(potential_points, gdf_stations)
    print(f"N. Lineas {len(potential_points)}")
    # Add altitude data for potential points
    # potential_points['altitude'] = get_altitude(potential_points)

    # Normalize features for potential points
    potential_points[features] = scaler.transform(potential_points[features])

    # Find similar environments
    n_neighbors = 5  # Number of similar environments to find
    nn = NearestNeighbors(n_neighbors=n_neighbors, metric='euclidean')
    nn.fit(gdf_top_stations[features])

    distances, indices = nn.kneighbors(potential_points[features])

    # Calculate similarity score (inverse of average distance)
    potential_points['similarity_score'] = 1 / distances.mean(axis=1)
    print(potential_points)
    # Filter out points too close to existing popular stations
    potential_points = potential_points[potential_points['dist_popular'] > min_distance]
    potential_points = potential_points[potential_points['dist_bike_station'] > min_distance]

    # Select top scoring points
    top_points = potential_points.sort_values('similarity_score', ascending=False).head(100)
    print("TOP POINTS:")
    print(top_points)
    # Cluster to avoid too many stations in one area
    coords = top_points.geometry.apply(lambda geom: (geom.x, geom.y)).tolist()
    print("COORDS:")
    print(coords)
    #db = DBSCAN(eps=100, min_samples=1).fit(coords) #use this if projected
    db = DBSCAN(eps=eps, min_samples=3).fit(coords)
    top_points['cluster'] = db.labels_

    # Select the highest scoring point from each cluster
    new_stations = top_points.loc[top_points.groupby('cluster')['similarity_score'].idxmax()]
    new_stations=new_stations.to_crs("EPSG:4326")
    print(f"Number of proposed new stations: {len(new_stations)}")
    print("\nTop 5 proposed locations:")
    print(new_stations[['geometry', 'similarity_score']].head())
    return new_stations

if __name__ == "__main__":
    get_similar_stations()