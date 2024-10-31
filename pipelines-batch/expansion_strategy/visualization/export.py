import pandas as pd
from bson import ObjectId
from pymongo import MongoClient

def clean_document(doc):
    """
    Convert ObjectId and other non-serializable types to serializable ones.
    """
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
        # Add any other necessary conversions here (e.g., for datetime)
    return doc

def doc_to_csv(collection_name, limit=None, fields=None):
    try:
        client = MongoClient("mongodb://mongodb:27017")
        db = client['bicing_db']
        collection = db[collection_name]

        # Use projection to limit fields if specified
        projection = {field: 1 for field in fields} if fields else None

        # Use limit to reduce the number of documents
        if limit:
            data = list(collection.find({}, projection).limit(limit))
        else:
            data = list(collection.find({}, projection))

        if not data:
            return None, f"No data found in collection {collection_name}"
        data = [clean_document(doc) for doc in data]

        df = pd.DataFrame(data)

        df.to_csv(f'tmp/{collection_name}.csv')
        return df,None

    except Exception as e:
        return None, f"Error processing {collection_name}: {str(e)}"

def filter_estacio_to_csv(df_to_filter):
    client = MongoClient("mongodb://mongodb:27017")
    db = client['bicing_db']
    collection = db['estacio']
    try:
        station_ids_to_exclude = df_to_filter['station_id'].tolist()

        pipeline = [
            # Match stage to filter out the station_ids present in the other DataFrame
            {"$match": {
                "station_id": {"$nin": station_ids_to_exclude}
            }},
            # Group by station_id and get the first document for each group
            {"$group": {
                "_id": "$station_id",
                "first_doc": {"$first": "$$ROOT"}
            }},
            # Replace the root with the first document
            {"$replaceRoot": {"newRoot": "$first_doc"}}
        ]

        # Execute the aggregation
        data = list(collection.aggregate(pipeline))

        df = pd.DataFrame(data)
        df.to_csv(f'tmp/estacio_filtered.csv')
        return df,None
    except Exception as e:
        return None, f"Error processing estacio: {str(e)}"


def main():
        # Load all required data
        gdf_top_stations, top_stations_error = doc_to_csv('top_stations',
                                                          fields=['name', 'station_id', 'geometry', 'reason','dist_education',
                                                                 'dist_shopping', 'dist_cpoi', 'dist_bus_stops',
                                                                 'dist_metro_stations', 'dist_bike_lanes',
                                                                 'dist_popular', 'dist_bike_station'])

        potential_stations, potential_error = doc_to_csv('proposed_station',
                                                         fields=['name', 'station_id', 'geometry', 'dist_education',
                                                                 'dist_shopping', 'dist_cpoi', 'dist_bus_stops',
                                                                 'dist_metro_stations', 'dist_bike_lanes',
                                                                 'dist_popular', 'dist_bike_station',
                                                                 'similarity_score', 'cluster'])

        gdf_bike_lanes, bike_lanes_error = doc_to_csv('bcn_bike_lanes', fields=['name', 'geometry'])

        gdf_metro_lines, metro_lines_error = doc_to_csv('metro_lines', fields=['name', 'geometry'])

        gdf_bus_lines, bus_lines_error = doc_to_csv('bus_lines', fields=['name', 'geometry'])

        gdf_stations, stations_error = filter_estacio_to_csv(gdf_top_stations)

        gdf_density, density_error = doc_to_csv('bcn_neighbourhood',
                                                fields=['name', 'geometry', 'commercial_density',
                                                        'school_density', 'poi_density'])


if __name__ == "__main__":
    main()