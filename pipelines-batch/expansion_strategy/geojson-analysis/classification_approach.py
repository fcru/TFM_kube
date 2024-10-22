import numpy as np
import geopandas as gpd
from scipy.spatial import cKDTree
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.cluster import DBSCAN
from shapely.geometry import Point as sh_point
from scipy.interpolate import LinearNDInterpolator
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.datasets import load_iris  # Example dataset
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.metrics import accuracy_score
from utilities import *

def nearest_dist(gdf1, gdf2):
    # Extract coordinates
    coords1 = np.array(list(gdf1.geometry.apply(lambda x: (x.x, x.y))))
    coords2 = np.array(list(gdf2.geometry.apply(lambda x: (x.x, x.y))))

    # Input validation
    if len(coords1) == 0 or len(coords2) == 0:
        return np.array([])

    tree = cKDTree(coords2)

    # Determine k based on whether we're comparing the same set of points
    same_points = gdf1.equals(gdf2)
    k = 2 if same_points else 1

    # Query for nearest neighbors
    distances, _ = tree.query(coords1, k=k, distance_upper_bound=np.inf)

    # If comparing different sets, return first distance directly
    if not same_points:
        return np.where(np.isinf(distances), np.nan, distances)

    # For same set comparison, handle self-matches
    distances = np.atleast_2d(distances)
    if distances.shape[1] == 1:
        distances = np.column_stack((distances, np.inf))

    # Use second nearest neighbor when first is self (distance = 0)
    result = np.where(distances[:, 0] == 0, distances[:, 1], distances[:, 0])

    return np.where(np.isinf(result), np.nan, result)


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
    result = collection_proposed.insert_many(data_to_insert)

    print(f"Inserted {len(result.inserted_ids)} new stations into MongoDB")

    client.close()


def get_similar_stations(projected=True):
    # Load data
    gdf_top_stations, top_stations_error = doc_to_gdf('top_stations',
                                                      fields=['name', 'station_id', 'geometry', 'reason'])
    #gdf_no_bikes = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'No Bikes' in x)]
    #gdf_no_docks = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'No Docks' in x)]
    #gdf_high = gdf_top_stations[gdf_top_stations['reason'].apply(lambda x: 'High Rotation' in x)]
    gdf_stations, stations_error = doc_to_gdf('all_stations',
                                                      fields=['name', 'station_id', 'geometry', 'Is_popular'])

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
        min_distance = 100
        eps = 50
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
    gdf_stations['dist_education'] = nearest_dist(gdf_stations, other_layers['Educational Centers'])
    gdf_stations['dist_shopping'] = nearest_dist(gdf_stations, other_layers['Commercial Census'])
    gdf_stations['dist_cpoi'] = nearest_dist(gdf_stations, other_layers['Cultural Points of Interest'])
    gdf_stations['dist_bus_stops'] = nearest_dist(gdf_stations, other_layers['Bus Stops'])
    gdf_stations['dist_metro_stations'] = nearest_dist(gdf_stations, other_layers['Metro Stations'])
    gdf_stations['dist_bike_lanes'] = gdf_stations.distance(other_layers["Bike Lanes"].unary_union)
    gdf_stations['dist_popular'] = nearest_dist(gdf_stations, gdf_top_stations)
    gdf_stations['dist_bike_station'] = nearest_dist(gdf_stations, gdf_stations)

    print(gdf_stations.describe())
    ## Interpolate altitude for potential points
    #known_points = np.array(list(gdf_stations.geometry.apply(lambda x: (x.x, x.y))))
    #known_altitudes = np.array(gdf_stations['altitude'])

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

    # Calculate features for potential points
    potential_points['dist_education'] = nearest_dist(potential_points, other_layers['Educational Centers'])
    potential_points['dist_shopping'] = nearest_dist(potential_points, other_layers['Commercial Census'])
    potential_points['dist_cpoi'] = nearest_dist(potential_points, other_layers['Cultural Points of Interest'])
    potential_points['dist_bus_stops'] = nearest_dist(potential_points, other_layers['Bus Stops'])
    potential_points['dist_metro_stations'] = nearest_dist(potential_points, other_layers['Metro Stations'])
    potential_points['dist_bike_lanes'] = potential_points.distance(other_layers["Bike Lanes"].unary_union)
    potential_points['dist_popular'] = nearest_dist(potential_points, gdf_top_stations)
    potential_points['dist_bike_station'] = nearest_dist(potential_points, gdf_stations)

    # Define features
    features = ['dist_education', 'dist_shopping', 'dist_bike_lanes', 'dist_cpoi', 'dist_bus_stops',
                'dist_metro_stations', 'dist_popular', 'dist_bike_station']

    columns_with_nan = potential_points.isna().any()
    print(columns_with_nan)
    potential_points = potential_points[potential_points['dist_popular'] > min_distance]
    potential_points = potential_points[potential_points['dist_bike_station'] > min_distance]
    nan_count_per_column = potential_points.isna().sum()
    print(nan_count_per_column)
    print("describe potential points filtered:")
    pd.set_option('display.float_format', '{:.6f}'.format)
    print(potential_points.describe())
    # Normalize features
    scaler = StandardScaler()
    X = scaler.fit_transform(gdf_stations[features])
    potential_points[features] = scaler.transform(potential_points[features])
    y = gdf_stations['Is_popular']
    y= y.replace({'yes': 1, 'no': 0})

    # Filter out points too close to existing popular stations

    print(f"Count of potential points:{len(potential_points)}")
    print(potential_points)
    # Classification

    # Split the data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Initialize models
    logreg = LogisticRegression(max_iter=1000)  # Increase max_iter to avoid convergence issues
    svm = SVC(kernel='linear')  # Linear kernel for SVM
    lda = LinearDiscriminantAnalysis()

    # Cross-validation to evaluate models
    logreg_scores = cross_val_score(logreg, X_train, y_train, cv=5)
    svm_scores = cross_val_score(svm, X_train, y_train, cv=5)
    lda_scores = cross_val_score(lda, X_train, y_train, cv=5)

    # Print the average accuracy for each model
    print(f"Logistic Regression Accuracy: {logreg_scores.mean():.4f}")
    print(f"SVM Accuracy: {svm_scores.mean():.4f}")
    print(f"LDA Accuracy: {lda_scores.mean():.4f}")

    # Train the models on the entire training set
    logreg.fit(X_train, y_train)
    svm.fit(X_train, y_train)
    lda.fit(X_train, y_train)

    # Predict on the test set
    y_pred_logreg = logreg.predict(X_test)
    y_pred_svm = svm.predict(X_test)
    y_pred_lda = lda.predict(X_test)

    # Calculate and print accuracy on the test set
    accuracy_logreg = accuracy_score(y_test, y_pred_logreg)
    accuracy_svm = accuracy_score(y_test, y_pred_svm)
    accuracy_lda = accuracy_score(y_test, y_pred_lda)

    print(f"Test Set Accuracy - Logistic Regression: {accuracy_logreg:.4f}")
    print(f"Test Set Accuracy - SVM: {accuracy_svm:.4f}")
    print(f"Test Set Accuracy - LDA: {accuracy_lda:.4f}")

    # Choose the best model based on test set accuracy
    best_model = max([(accuracy_logreg, logreg),
                      (accuracy_svm, svm),
                      (accuracy_lda, lda)], key=lambda x: x[0])

    best_model_object = best_model[1]  # Now this is the actual model object (e.g., logreg, svm, lda)


    # Select the highest scoring point from each cluster
    potential_points_features = potential_points[features]
    classified_points = best_model_object.predict(potential_points_features)
    new_stations = potential_points[classified_points == 1]
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