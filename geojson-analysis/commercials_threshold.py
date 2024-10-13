from pymongo import MongoClient
from datetime import datetime
import json
from typing import List, Dict, Any


def connect_to_mongodb() -> MongoClient:
    """Create MongoDB connection"""
    client = MongoClient('mongodb://mongodb:27017/')
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


def convert_geometries(db) -> None:
    """Convert WKT geometries to GeoJSON in both collections"""
    print("Converting geometries...")

    # Convert neighborhoods
    for doc in db.bcn_neighbourhood.find({'geometry': {'$type': 'string'}}):
        geojson = convert_wkt_to_geojson(doc['geometry'])
        if geojson:
            db.bcn_neighbourhood.update_one(
                {'_id': doc['_id']},
                {
                    '$set': {
                        'geometry': geojson,
                        'original_wkt': doc['geometry']
                    }
                }
            )

    # Convert commercials
    for doc in db.all_commercials.find({'geometry': {'$type': 'string'}}):
        geojson = convert_wkt_to_geojson(doc['geometry'])
        if geojson:
            db.all_commercials.update_one(
                {'_id': doc['_id']},
                {
                    '$set': {
                        'geometry': geojson,
                        'original_wkt': doc['geometry']
                    }
                }
            )

    print("Geometry conversion completed")


def create_indexes(db) -> None:
    """Create necessary spatial indexes"""
    print("Creating indexes...")
    db.bcn_neighbourhood.create_index([("geometry", "2dsphere")])
    db.all_commercials.create_index([("geometry", "2dsphere")])
    print("Indexes created")


def calculate_classifications(db) -> None:
    """Calculate and update commercial density classifications"""
    print("Calculating classifications...")

    # First, get all neighborhoods and count commercials for each
    neighbourhoods_data = []

    for neighbourhood in db.bcn_neighbourhood.find():
        commercial_count = db.all_commercials.count_documents({
            'geometry': {
                '$geoWithin': {
                    '$geometry': neighbourhood['geometry']
                }
            }
        })

        neighbourhoods_data.append({
            '_id': neighbourhood['_id'],
            'name': neighbourhood['name'],
            'count': commercial_count
        })

    if neighbourhoods_data:
        # Sort counts to calculate thresholds
        counts = sorted(n['count'] for n in neighbourhoods_data)
        low_threshold = counts[int(len(counts) * 0.25)]
        high_threshold = counts[int(len(counts) * 0.60)]
        very_high_threshold = counts[int(len(counts) * 0.75)]

        print(f"\nClassification thresholds:")
        print(f"Low: {low_threshold} commercials")
        print(f"High: {high_threshold} commercials")

        # Update each neighbourhood
        for n in neighbourhoods_data:
            if n['count'] <= low_threshold:
                classification = 'low'
            elif n['count'] <= high_threshold:
                classification = 'medium'
            elif n['count'] <= very_high_threshold:
                classification = 'high'
            else:
                classification = 'very_high'

            db.bcn_neighbourhood.update_one(
                {'_id': n['_id']},
                {
                    '$set': {
                        'commercial_density': {
                            'classification': classification,
                            'count': n['count']
                        }
                    }
                }
            )

        print(f"\nUpdated {len(neighbourhoods_data)} neighbourhoods")

        # Print results
        print("\nResults:")
        for doc in db.bcn_neighbourhood.find(
                {},
                {
                    'name': 1,
                    'commercial_density.classification': 1,
                    'commercial_density.count': 1
                }
        ).sort('commercial_density.count', -1):
            print(f"{doc['name']}: {doc['commercial_density']['classification']} "
                  f"({doc['commercial_density']['count']} commercials)")


def main():
    """Main function to run the complete process"""
    try:
        # Connect to MongoDB
        client = connect_to_mongodb()
        db = client.bicing_db  # Use your actual database name

        print("Starting commercial density classification process...")

        # Run all steps
        convert_geometries(db)
        create_indexes(db)
        calculate_classifications(db)

        print("\nProcess completed successfully")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        client.close()


if __name__ == "__main__":
    main()