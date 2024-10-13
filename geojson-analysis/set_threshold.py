from pymongo import MongoClient
from datetime import datetime
import json
from typing import List, Dict, Any


def connect_to_mongodb(connection_string: str = 'mongodb://mongodb:27017/') -> MongoClient:
    """Create MongoDB connection"""
    client = MongoClient(connection_string)
    return client


def convert_wkt_to_geojson(wkt: str) -> Dict[str, Any]:
    """Convert WKT string to GeoJSON object"""
    if wkt.startswith('POLYGON'):
        coords_str = wkt.replace('POLYGON ((', '').replace('))', '')
        coordinates = [
            [float(x) for x in pair.split()]
            for pair in coords_str.split(', ')
        ]
        return {
            'type': 'Polygon',
            'coordinates': [coordinates]
        }
    elif wkt.startswith('POINT'):
        coords_str = wkt.replace('POINT(', '').replace(')', '')
        lng, lat = map(float, coords_str.split())
        return {
            'type': 'Point',
            'coordinates': [lng, lat]
        }
    return None


def convert_geometries(
    db,
    container_collection: str,
    point_collection: str,
    geometry_field: str = 'geometry'
) -> None:
    """Convert WKT geometries to GeoJSON in both collections"""
    print("Converting geometries...")

    # Convert containers (e.g., neighborhoods)
    for doc in db[container_collection].find({geometry_field: {'$type': 'string'}}):
        geojson = convert_wkt_to_geojson(doc[geometry_field])
        if geojson:
            db[container_collection].update_one(
                {'_id': doc['_id']},
                {
                    '$set': {
                        geometry_field: geojson,
                        'original_wkt': doc[geometry_field]
                    }
                }
            )

    # Convert points (e.g., commercials)
    for doc in db[point_collection].find({geometry_field: {'$type': 'string'}}):
        geojson = convert_wkt_to_geojson(doc[geometry_field])
        if geojson:
            db[point_collection].update_one(
                {'_id': doc['_id']},
                {
                    '$set': {
                        geometry_field: geojson,
                        'original_wkt': doc[geometry_field]
                    }
                }
            )

    print("Geometry conversion completed")


def create_indexes(
    db,
    container_collection: str,
    point_collection: str,
    geometry_field: str = 'geometry'
) -> None:
    """Create necessary spatial indexes"""
    print("Creating indexes...")
    db[container_collection].create_index([(geometry_field, "2dsphere")])
    db[point_collection].create_index([(geometry_field, "2dsphere")])
    print("Indexes created")


def calculate_classifications(
    db,
    container_collection: str,
    point_collection: str,
    geometry_field: str = 'geometry',
    name_field: str = 'name',
    classification_field: str = 'density_classification',
    low_threshold_percentile: float = 0.25,
    high_threshold_percentile: float = 0.60,
    very_high_threshold_percentile: float = 0.75
) -> None:
    """Calculate and update point density classifications"""
    print("Calculating classifications...")

    # First, get all containers and count points for each
    containers_data = []

    for container in db[container_collection].find():
        point_count = db[point_collection].count_documents({
            geometry_field: {
                '$geoWithin': {
                    '$geometry': container[geometry_field]
                }
            }
        })

        containers_data.append({
            '_id': container['_id'],
            'name': container[name_field],
            'count': point_count
        })

    if containers_data:
        # Sort counts to calculate thresholds
        counts = sorted(n['count'] for n in containers_data)
        low_threshold = counts[int(len(counts) * low_threshold_percentile)]
        high_threshold = counts[int(len(counts) * high_threshold_percentile)]
        very_high_threshold = counts[int(len(counts) * very_high_threshold_percentile)]

        print(f"\nClassification thresholds:")
        print(f"Low: {low_threshold} points")
        print(f"High: {high_threshold} points")

        # Update each container
        for n in containers_data:
            if n['count'] <= low_threshold:
                classification = 'low'
            elif n['count'] <= high_threshold:
                classification = 'medium'
            elif n['count'] <= very_high_threshold:
                classification = 'high'
            else:
                classification = 'very_high'

            db[container_collection].update_one(
                {'_id': n['_id']},
                {
                    '$set': {
                        classification_field: {
                            'classification': classification,
                            'count': n['count']
                        }
                    }
                }
            )

        print(f"\nUpdated {len(containers_data)} containers")

        # Print results
        print("\nResults:")
        for doc in db[container_collection].find(
                {},
                {
                    name_field: 1,
                    f'{classification_field}.classification': 1,
                    f'{classification_field}.count': 1
                }
        ).sort(f'{classification_field}.count', -1):
            print(f"{doc[name_field]}: {doc[classification_field]['classification']} "
                  f"({doc[classification_field]['count']} points)")


def set_threshold(
    connection_string: str = 'mongodb://mongodb:27017/',
    database_name: str = 'bicing_db',
    container_collection: str = 'bcn_neighbourhood',
    point_collection: str = 'all_commercials',
    geometry_field: str = 'geometry',
    name_field: str = 'name',
    classification_field: str = 'commercial_density'
):
    """Main function to run the complete process"""
    try:
        # Connect to MongoDB
        client = connect_to_mongodb(connection_string)
        db = client[database_name]

        print("Starting density classification process...")

        # Run all steps
        convert_geometries(db, container_collection, point_collection, geometry_field)
        create_indexes(db, container_collection, point_collection, geometry_field)
        calculate_classifications(
            db,
            container_collection,
            point_collection,
            geometry_field,
            name_field,
            classification_field
        )

        print("\nProcess completed successfully")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        client.close()


if __name__ == "__main__":
    # Example usage with default parameters (original Barcelona case)
    set_threshold()


    set_threshold(
        container_collection='bcn_neighbourhood',
        point_collection='educative_centers',
        geometry_field='geometry',
        name_field='name',
        classification_field='school_density'
    )

    set_threshold(
        container_collection='bcn_neighbourhood',
        point_collection='cultural_points_of_interests',
        geometry_field='geometry',
        name_field='name',
        classification_field='poi_density'
    )


