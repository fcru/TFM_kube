import pandas as pd
import json
from pymongo import MongoClient
import geopandas as gpd
from utilities import *

def geometry_to_string(geom):
    if isinstance(geom, dict):
        return json.dumps(geom)
    return str(geom)

def string_to_geometry(geom_str):
    try:
        return json.loads(geom_str)
    except json.JSONDecodeError:
        return geom_str

def to_gdf(df):
    if 'geometry' in df.columns:
        df['geometry'] = df['geometry'].apply(safe_shape)
    gdf = gpd.GeoDataFrame(df, geometry='geometry').dropna(subset=['geometry'])
    gdf.set_crs(epsg=4326, inplace=True)
    return gdf

def find_top_stations():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['bicing_db']
    collection = db['estacio']  # Use 'estacio' collection

    # Aggregation for num_bikes_available = 0
    pipeline_bikes = [
        # Unwind the status array
        {"$unwind": "$status"},

        # Match documents where num_bikes_available is 0
        {"$match": {"status.num_bikes_available": 0}},

        # Group by station, year, and month
        {
            "$group": {
                "_id": {
                    "station_id": "$station_id",
                    "year": {"$year": "$date"},
                    "month": {"$month": "$date"}
                },
                "count": {"$sum": 1},  # Count occurrences
                "name": {"$first": "$name"},
                "geometry": {"$first": "$geometry"},
                "altitude": {"$first": "$altitude"},
                "address": {"$first": "$address"},
                "post_code": {"$first": "$post_code"}
            }
        },

        # Group by station to calculate average monthly count
        {
            "$group": {
                "_id": {
                    "station_id": "$_id.station_id",
                    "name": "$name",
                    "geometry": "$geometry",
                    "altitude": "$altitude",
                    "address": "$address",
                    "post_code": "$post_code"
                },
                "avg_monthly_count": {"$avg": "$count"},
                "months_count": {"$sum": 1}  # Count of months with data
            }
        },

        # Project final output
        {
            "$project": {
                "_id": 0,
                "station_id": "$_id.station_id",
                "name": "$_id.name",
                "geometry": "$_id.geometry",
                "altitude": "$_id.altitude",
                "address": "$_id.address",
                "post_code": "$_id.post_code",
                "avg_monthly_count": 1,
                "months_count": 1
            }
        },

        # Sort by average monthly count in descending order
        {"$sort": {"avg_monthly_count": -1}},

        # Limit to top 10 results
        {"$limit": 10}
    ]

    # Aggregation for num_docks_available = 0
    pipeline_docks = [
        # Unwind the status array
        {"$unwind": "$status"},

        # Match documents where num_docks_available is 0
        {"$match": {"status.num_docks_available": 0}},

        # Group by station, year, and month
        {
            "$group": {
                "_id": {
                    "station_id": "$station_id",
                    "year": {"$year": "$date"},
                    "month": {"$month": "$date"}
                },
                "count": {"$sum": 1},  # Count occurrences
                "name": {"$first": "$name"},
                "geometry": {"$first": "$geometry"},
                "altitude": {"$first": "$altitude"},
                "address": {"$first": "$address"},
                "post_code": {"$first": "$post_code"}
            }
        },

        # Group by station to calculate average monthly count
        {
            "$group": {
                "_id": {
                    "station_id": "$_id.station_id",
                    "name": "$name",
                    "geometry": "$geometry",
                    "altitude": "$altitude",
                    "address": "$address",
                    "post_code": "$post_code"
                },
                "avg_monthly_count": {"$avg": "$count"},
                "months_count": {"$sum": 1}  # Count of months with data
            }
        },

        # Project final output
        {
            "$project": {
                "_id": 0,
                "station_id": "$_id.station_id",
                "name": "$_id.name",
                "geometry": "$_id.geometry",
                "altitude": "$_id.altitude",
                "address": "$_id.address",
                "post_code": "$_id.post_code",
                "avg_monthly_count": 1,
                "months_count": 1
            }
        },

        # Sort by average monthly count in descending order
        {"$sort": {"avg_monthly_count": -1}},

        # Limit to top 10 results
        {"$limit": 10}
    ]

    # MongoDB aggregation pipeline
    pipeline_high_rotation = [
        # Unwind the status array
        {"$unwind": "$status"},

        # Group by station and date to calculate daily rotation
        {
            "$group": {
                "_id": {
                    "station_id": "$station_id",
                    "date": "$date",
                    "name": "$name",
                    "geometry": "$geometry",
                    "altitude": "$altitude",
                    "address": "$address",
                    "post_code": "$post_code"
                },
                "hourlyData": {
                    "$push": {
                        "hour": "$status.hour",
                        "bikes": "$status.num_bikes_available"
                    }
                }
            }
        },

        # Calculate daily rotation
        {
            "$project": {
                "_id": 0,
                "station_id": "$_id.station_id",
                "date": "$_id.date",
                "name": "$_id.name",
                "geometry": "$_id.geometry",
                "altitude": "$_id.altitude",
                "address": "$_id.address",
                "post_code": "$_id.post_code",
                "dailyRotation": {
                    "$reduce": {
                        "input": {"$range": [1, 24]},
                        "initialValue": 0,
                        "in": {
                            "$add": [
                                "$$value",
                                {
                                    "$abs": {
                                        "$subtract": [
                                            {"$arrayElemAt": ["$hourlyData.bikes", "$$this"]},
                                            {"$arrayElemAt": ["$hourlyData.bikes", {"$subtract": ["$$this", 1]}]}
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        },

        # Group by station to calculate average daily rotation
        {
            "$group": {
                "_id": {
                    "station_id": "$station_id",
                    "name": "$name",
                    "geometry": "$geometry",
                    "altitude": "$altitude",
                    "address": "$address",
                    "post_code": "$post_code"
                },
                "total_rotation": {"$sum": "$dailyRotation"},
                "count": {"$sum": 1}
            }
        },

        # Project final output
        {
            "$project": {
                "_id": 0,
                "station_id": "$_id.station_id",
                "name": "$_id.name",
                "geometry": "$_id.geometry",
                "altitude": "$_id.altitude",
                "address": "$_id.address",
                "post_code": "$_id.post_code",
                "count": 1,
                "avg_daily_rotation": {"$divide": ["$total_rotation", "$count"]}
            }
        },

        # Sort by average daily rotation in descending order
        {"$sort": {"avg_daily_rotation": -1}},
        # Limit to top 10 results
        {"$limit": 10}
    ]


    # Execute the aggregation query with allowDiskUse=True
    results_high = list(collection.aggregate(pipeline_high_rotation, allowDiskUse=True))

    # Execute the aggregation for bikes
    results_bikes = list(collection.aggregate(pipeline_bikes))

    # Execute the aggregation for docks
    results_docks = list(collection.aggregate(pipeline_docks))

    # Convert results to DataFrames
    df_bikes = pd.DataFrame(results_bikes)
    df_docks = pd.DataFrame(results_docks)
    df_high = pd.DataFrame(results_high)

    # Flatten the '_id' field into separate columns for DataFrames
    #df_bikes[['station_id', 'year', 'month']] = pd.json_normalize(df_bikes['_id'])
    #df_docks[['station_id', 'year', 'month']] = pd.json_normalize(df_docks['_id'])
    #df_high[['station_id', 'year', 'month']] = pd.json_normalize(df_high['_id'])

    final_bikes = df_bikes[['station_id', 'name', 'geometry', 'altitude', 'address', 'post_code']]
    final_docks = df_docks[['station_id', 'name', 'geometry', 'altitude', 'address', 'post_code']]
    final_high = df_high[['station_id', 'name', 'geometry', 'altitude', 'address', 'post_code']]

    print(final_bikes)

    #final_bikes['geometry'] = final_bikes['geometry'].apply(geometry_to_string)
    #final_docks['geometry'] = final_docks['geometry'].apply(geometry_to_string)
    #final_high['geometry'] = final_high['geometry'].apply(geometry_to_string)
#
    ## Drop duplicates
    #final_bikes = final_bikes.drop_duplicates()
    #final_docks = final_docks.drop_duplicates()
    #final_high = final_high.drop_duplicates()
#
    # For final_bikes
    final_bikes = final_bikes.copy()
    final_bikes['reason'] = "No Bikes"

    # For final_docks
    final_docks = final_docks.copy()
    final_docks['reason'] = "No Docks"

    # For final_high
    final_high = final_high.copy()
    final_high['reason'] = "High Rotation"

    merged_df = pd.concat([final_bikes,final_docks,final_high], ignore_index=True)

    print(merged_df)

    # Merge DataFrames and keep all initial fields
    final_df = merged_df.groupby('station_id', as_index=False).agg({
        'reason': lambda x: list(x),  # Combine reasons into an array
        'name': 'first',  # Get first occurrence of name
        'geometry': 'first',  # Get first occurrence of geometry
        'altitude': 'first',  # Get first occurrence of altitude
        'address': 'first',  # Get first occurrence of address
        'post_code': 'first'  # Get first occurrence of post_code
    })

    print(final_df)

    top_collection = db['top_stations']

    # Convert DataFrame to dictionary format
    data_to_insert = final_df.to_dict(orient='records')
    top_collection.drop()
    top_collection.insert_many(data_to_insert)

    print("Data has been successfully saved to MongoDB.")

    collection_update = db['all_stations']
    # Step 1: Add Is_popular field with default value "No" to all documents
    result = collection_update.update_many(
        {"Is_popular": {"$exists": False}},
        {"$set": {"Is_popular": "No"}}
    )
    print(f"Added Is_popular field to {result.modified_count} documents")

    # Step 2: Update Is_popular to "Yes" for stations in the DataFrame
    update_count = 0
    for station_id in final_df['station_id']:
        result = collection_update.update_one(
            {'station_id': station_id},
            {'$set': {'Is_popular': 'Yes'}}
        )
        update_count += result.modified_count

    print(f"Updated {update_count} stations to Is_popular: Yes")

if __name__ == "__main__":
    find_top_stations()