import pandas as pd
import json
from pymongo import MongoClient
import geopandas as gpd
from utilities import *

def get_top_10_stations(df):
    top_stations = df.groupby(['year', 'station_id']).agg({'count': 'sum'}).reset_index()
    top_10 = top_stations.sort_values(['year', 'count'], ascending=[True, False]).groupby('year').head(10)
    return top_10

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
        {"$unwind": "$status"},
        {"$match": {"status.num_bikes_available": 0}},
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
        }
    ]

    # Aggregation for num_docks_available = 0
    pipeline_docks = [
        {"$unwind": "$status"},
        {"$match": {"status.num_docks_available": 0}},
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
        }
    ]

    # MongoDB aggregation pipeline
    pipeline_high_rotation = [
        # Unwind the status array
        {"$unwind": "$status"},

        # Group by station, date, and hour to calculate hourly changes
        {
            "$group": {
                "_id": {
                    "station_id": "$station_id",
                    "date": "$date",
                    "hour": "$status.hour"
                },
                "bikes_change": {"$max": {"$abs": "$status.num_bikes_available"}},
                "name": {"$first": "$name"},
                "geometry": {"$first": "$geometry"},
                "altitude": {"$first": "$altitude"},
                "address": {"$first": "$address"},
                "post_code": {"$first": "$post_code"}
            }
        },

        # Group by station and date to calculate daily rotation
        {
            "$group": {
                "_id": {
                    "station_id": "$_id.station_id",
                    "date": "$_id.date"
                },
                "daily_rotation": {"$sum": "$bikes_change"},
                "name": {"$first": "$name"},
                "geometry": {"$first": "$geometry"},
                "altitude": {"$first": "$altitude"},
                "address": {"$first": "$address"},
                "post_code": {"$first": "$post_code"}
            }
        },

        # Group by station, year, and month
        {
            "$group": {
                "_id": {
                    "station_id": "$_id.station_id",
                    "year": {"$year": "$_id.date"},
                    "month": {"$month": "$_id.date"}
                },
                "count": {"$sum": 1},
                "total_rotation": {"$sum": "$daily_rotation"},
                "name": {"$first": "$name"},
                "geometry": {"$first": "$geometry"},
                "altitude": {"$first": "$altitude"},
                "address": {"$first": "$address"},
                "post_code": {"$first": "$post_code"}
            }
        },

        # Calculate average daily rotation
        {
            "$project": {
                "count": 1,
                "name": 1,
                "geometry": 1,
                "altitude": 1,
                "address": 1,
                "post_code": 1,
                "avg_daily_rotation": {"$divide": ["$total_rotation", "$count"]}
            }
        },

        # Sort by year, month, and average daily rotation (descending)
        {
            "$sort": SON([
                ("_id.year", 1),
                ("_id.month", 1),
                ("avg_daily_rotation", -1)
            ])
        }
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
    df_bikes[['station_id', 'year', 'month']] = pd.json_normalize(df_bikes['_id'])
    df_docks[['station_id', 'year', 'month']] = pd.json_normalize(df_docks['_id'])
    df_high[['station_id', 'year', 'month']] = pd.json_normalize(df_high['_id'])

    # Drop the '_id' field
    df_bikes.drop(columns=['_id'], inplace=True)
    df_docks.drop(columns=['_id'], inplace=True)
    df_high.drop(columns=['_id','count'], inplace=True)
    df_high.rename(columns={'avg_daily_rotation': 'count'}, inplace=True)

    # Get top 10 stations for bikes and docks
    top_10_bikes = get_top_10_stations(df_bikes)
    top_10_docks = get_top_10_stations(df_docks)
    top_10_high = get_top_10_stations(df_high)

    ## Create a DataFrame to count occurrences of each station_id in the top 10 across years
    #years_bikes = top_10_bikes.groupby('station_id')['year'].apply(list).reset_index(name='years')
    #years_docks = top_10_docks.groupby('station_id')['year'].apply(list).reset_index(name='years')
    #years_high = top_10_high.groupby('station_id')['year'].apply(list).reset_index(name='years')
#
    ## Count the number of years each station appears in the top 10
    #years_bikes['year_count'] = years_bikes['years'].apply(len)
    #years_docks['year_count'] = years_docks['years'].apply(len)
    #years_high['year_count'] = years_high['years'].apply(len)
#
    ## Filter stations that appear in the top 10 for at least 2 years
    #years_bikes_filtered = years_bikes[years_bikes['year_count'] >= 2]
    #years_docks_filtered = years_docks[years_docks['year_count'] >= 2]
    #years_high_filtered = years_high[years_high['year_count'] >= 2]
#
    #final_bikes = df_bikes[df_bikes['station_id'].isin(years_bikes_filtered['station_id'])]
    #final_docks = df_docks[df_docks['station_id'].isin(years_docks_filtered['station_id'])]
    #final_high = df_high[df_high['station_id'].isin(years_high_filtered['station_id'])]

    final_bikes = df_bikes[df_bikes['station_id'].isin(top_10_bikes['station_id'])]
    final_docks = df_docks[df_docks['station_id'].isin(top_10_docks['station_id'])]
    final_high = df_high[df_high['station_id'].isin(top_10_high['station_id'])]

    final_bikes = final_bikes[['station_id', 'name', 'geometry', 'altitude', 'address', 'post_code']]
    final_docks = final_docks[['station_id', 'name', 'geometry', 'altitude', 'address', 'post_code']]
    final_high = final_high[['station_id', 'name', 'geometry', 'altitude', 'address', 'post_code']]

    final_bikes['geometry'] = final_bikes['geometry'].apply(geometry_to_string)
    final_docks['geometry'] = final_docks['geometry'].apply(geometry_to_string)
    final_high['geometry'] = final_high['geometry'].apply(geometry_to_string)

    # Drop duplicates
    final_bikes = final_bikes.drop_duplicates()
    final_docks = final_docks.drop_duplicates()
    final_high = final_high.drop_duplicates()

    # Convert geometry back to original format
    final_bikes['geometry'] = final_bikes['geometry'].apply(string_to_geometry)
    final_bikes['reason'] = "No Bikes"
    final_docks['geometry'] = final_docks['geometry'].apply(string_to_geometry)
    final_docks['reason'] = "No Docks"
    final_high['geometry'] = final_high['geometry'].apply(string_to_geometry)
    final_high['reason'] = "High Rotation"
    # Add the years column to final DataFrames
    #final_bikes = final_bikes.merge(years_bikes_filtered[['station_id', 'years']], on='station_id', how='left')
    #final_docks = final_docks.merge(years_docks_filtered[['station_id', 'years']], on='station_id', how='left')
    #final_high = final_high.merge(years_docks_filtered[['station_id', 'years']], on='station_id', how='left')

    merged_df = pd.concat([final_bikes,final_docks,final_high], ignore_index=True)

    # Merge DataFrames and keep all initial fields
    final_df = merged_df.groupby('station_id', as_index=False).agg({
        'reason': lambda x: list(x),  # Combine reasons into an array
        'name': 'first',  # Get first occurrence of name
        'geometry': 'first',  # Get first occurrence of geometry
        'altitude': 'first',  # Get first occurrence of altitude
        'address': 'first',  # Get first occurrence of address
        'post_code': 'first'  # Get first occurrence of post_code
    })


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
    # Assuming you have a DataFrame named 'popular_stations' with a column 'station_id'

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