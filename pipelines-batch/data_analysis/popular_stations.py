from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb://mongodb:27017/')
db = client['bicing_db']
collection = db['estacio']  # Use 'estacio' collection

# MongoDB aggregation pipeline
pipeline = [
    {"$unwind": "$status"},
    {"$sort": {
        "date": 1,  # Sort by the date field
        "status.hour": 1
    }},
    {"$group": {
        "_id": {
            "station_id": "$station_id",
            "date": "$date"  # Group by station_id and date
        },
        "firstHourDocksAvailable": { "$first": { "$toInt": "$status.num_docks_available" } },
        "name": { "$first": "$name" },  # Station name
        "lat": { "$first": "$lat" },  # Latitude
        "lon": { "$first": "$lon" },  # Longitude
        "altitude": { "$first": "$altitude" },  # Altitude
        "statuses": { "$push": "$status" }
    }},
    {"$unwind": "$statuses"},
    {"$addFields": {
        "statuses.num_docks_available": { "$toInt": "$statuses.num_docks_available" },  # Cast to int
        "bikesRented": {
            "$subtract": [
                "$firstHourDocksAvailable",
                { "$toInt": "$statuses.num_docks_available" }  # Subtract from baseline
            ]
        }
    }},
    {"$group": {
        "_id": {
            "station_id": "$_id.station_id",
            "date": "$_id.date"
        },
        "name": { "$first": "$name" },  # Station name
        "lat": { "$first": "$lat" },  # Latitude
        "lon": { "$first": "$lon" },  # Longitude
        "altitude": { "$first": "$altitude" },  # Altitude
        "totalBikesRented": { "$sum": "$bikesRented" }
    }},
    {"$sort": { "totalBikesRented": -1 }},
    {"$group": {
        "_id": {
            "year": { "$year": "$_id.date" },
            "month": { "$month": "$_id.date" }
        },
        "topStations": {
            "$push": {
                "station_id": "$_id.station_id",
                "name": "$name",
                "totalBikesRented": "$totalBikesRented",
                "lat": "$lat",
                "lon": "$lon",
                "altitude": "$altitude"
            }
        }
    }},
    {"$project": {
        "topStations": { "$slice": ["$topStations", 5] }
    }}
]

# Execute the aggregation query with allowDiskUse=True
result = collection.aggregate(pipeline, allowDiskUse=True)

# Convert result to a list
result_list = []
for doc in result:
    year = doc['_id']['year']
    month = doc['_id']['month']
    for station in doc['topStations']:
        # Get the lon, lat, and altitude values and handle missing data
        lon = station.get('lon')
        lat = station.get('lat')
        altitude = station.get('altitude')

        if lon is None or lat is None or altitude is None:
            print(
                f"Missing or invalid coordinates for station_id {station.get('station_id', 'Unknown')}: lon={lon}, lat={lat}, altitude={altitude}")
            continue  # Skip this station and proceed with the next one
        result_list.append({
            "year": year,
            "month": month,
            "station_id": station.get('station_id', 'Unknown'),
            "name": station.get('name', 'Unknown'),
            "usage_count": station.get('totalBikesRented', 0),
            "lon": lon,
            "lat":lat,
            "alt":altitude
        })

# Convert the list of dictionaries to a pandas DataFrame
df = pd.DataFrame(result_list)

# Add new columns with specified values
df['popularByUsage'] = 'Yes'
df['HighFullDocksRate'] = 'No'
df['HighEmptyDocksRate'] = 'No'

# Sort the DataFrame by year and month ascending
df = df.sort_values(by=['year', 'month'], ascending=[True, True])

# Display the DataFrame
print("df top usage:")
print(df)

# Query for stations with 0 docks availability
pipeline_0_docks = [
    {"$unwind": "$status"},
    {"$match": {"status.num_docks_available": 0}},
    {"$group": {
        "_id": {
            "station_id": "$station_id",
            "date": "$date"  # Group by station_id and date
        },
        "count_zero_docks": {"$sum": 1},
        "name": {"$first": "$name"},
        "lat": {"$first": "$lat"},
        "lon": {"$first": "$lon"},
        "altitude": {"$first": "$altitude"}
    }},
    {"$group": {
        "_id": {
            "station_id": "$_id.station_id",
            "date": "$_id.date"
        },
        "total_zero_docks": {"$sum": "$count_zero_docks"},
        "name": {"$first": "$name"},
        "lat": {"$first": "$lat"},
        "lon": {"$first": "$lon"},
        "altitude": {"$first": "$altitude"}
    }},
    {"$sort": {"total_zero_docks": -1}},
    {"$group": {
        "_id": {
            "year": { "$year": "$_id.date" },
            "month": { "$month": "$_id.date" }
        },
        "topStations": {
            "$push": {
                "station_id": "$_id.station_id",
                "total_zero_docks": "$total_zero_docks",
                "name": "$name",
                "lat": "$lat",
                "lon": "$lon",
                "altitude": "$altitude"
            }
        }
    }},
    {"$project": {
        "topStations": {"$slice": ["$topStations", 5]}
    }}
]

# Execute the aggregation query with allowDiskUse=True for 0 docks availability
result_0_docks = collection.aggregate(pipeline_0_docks, allowDiskUse=True)

# Convert result to a list
result_0_docks_list = []
for doc in result_0_docks:
    year = doc['_id']['year']
    month = doc['_id']['month']
    for station in doc['topStations']:
        lon = station.get('lon')
        lat = station.get('lat')
        altitude = station.get('altitude')

        if lon is None or lat is None or altitude is None:
            print(
                f"Missing coordinates for station_id {station.get('station_id', 'Unknown')}: lon={lon}, lat={lat}, altitude={altitude}")
            continue  # Skip this station

        result_0_docks_list.append({
            "year": year,
            "month": month,
            "station_id": station.get('station_id', 'Unknown'),
            "name": station.get('name', 'Unknown'),
            "usage_count": station.get('total_zero_docks', 0),
            "lon": lon,
            "lat":lat,
            "alt":altitude,
            "popularByUsage": 'No',
            "HighFullDocksRate": 'Yes',
            "HighEmptyDocksRate": 'No'
        })

# Convert the list of dictionaries to a pandas DataFrame
df_0_docks = pd.DataFrame(result_0_docks_list)
print("df 0 docks:")
print(df_0_docks)
df_0_docks = df_0_docks.sort_values(by=['year', 'month'], ascending=[True, True])



pipeline_100_docks = [
    {"$unwind": "$status"},
    {"$match": {"status.num_bikes_available": 0}},
    {"$group": {
        "_id": {
            "station_id": "$station_id",
            "date": "$date"  # Group by station_id and date
        },
        "count_full_docks": {"$sum": 1},  # Changed from usage_count to count_full_docks
        "name": {"$first": "$name"},
        "lat": {"$first": "$lat"},
        "lon": {"$first": "$lon"},
        "altitude": {"$first": "$altitude"}
    }},
    {"$group": {
        "_id": {
            "station_id": "$_id.station_id",
            "year": {"$year": "$_id.date"},
            "month": {"$month": "$_id.date"}
        },
        "total_full_docks": {"$sum": "$count_full_docks"},  # This now correctly sums count_full_docks
        "name": {"$first": "$name"},
        "lat": {"$first": "$lat"},
        "lon": {"$first": "$lon"},
        "altitude": {"$first": "$altitude"}
    }},
    {"$sort": {"total_full_docks": -1}},
    {"$group": {
        "_id": {
            "year": "$_id.year",
            "month": "$_id.month"
        },
        "topStations": {
            "$push": {
                "station_id": "$_id.station_id",
                "total_full_docks": "$total_full_docks",
                "name": "$name",
                "lat": "$lat",
                "lon": "$lon",
                "altitude": "$altitude"
            }
        }
    }},
    {"$project": {
        "topStations": {"$slice": ["$topStations", 5]}
    }}
]

# Execute the aggregation query with allowDiskUse=True for 100% docks availability
result_100_docks = collection.aggregate(pipeline_100_docks, allowDiskUse=True)

# Convert result to a list
result_100_docks_list = []
for doc in result_100_docks:
    year = doc['_id']['year']
    month = doc['_id']['month']
    for station in doc['topStations']:
        lon = station.get('lon')
        lat = station.get('lat')
        altitude = station.get('altitude')

        if lon is None or lat is None or altitude is None:
            print(
                f"Missing coordinates for station_id {station.get('station_id', 'Unknown')}: lon={lon}, lat={lat}, altitude={altitude}")
            continue  # Skip this station

        result_100_docks_list.append({
            "year": year,
            "month": month,
            "station_id": station.get('station_id', 'Unknown'),
            "name": station.get('name', 'Unknown'),
            "usage_count": station.get('total_full_docks', 0),
            "lon": lon,
            "lat": lat,
            "alt": altitude,
            "popularByUsage": 'No',
            "HighFullDocksRate": 'No',
            "HighEmptyDocksRate": 'Yes'
        })

# Convert the list of dictionaries to a pandas DataFrame
df_100_docks = pd.DataFrame(result_100_docks_list)
df_100_docks = df_100_docks.sort_values(by=['year', 'month'], ascending=[True, True])

print(df_100_docks)

# Concatenate all DataFrames
df_combined = pd.concat([df, df_0_docks, df_100_docks], ignore_index=True)

# Create GeoJSON location
df_combined['location'] = df_combined.apply(lambda row: {
    'type': 'Point',
    'coordinates': [float(row['lon']), float(row['lat']), float(row['alt'])]
}, axis=1)

# Drop the original coordinate columns
df_combined = df_combined.drop(columns=['lon', 'lat', 'alt'])

# Sort the combined DataFrame
df_combined = df_combined.sort_values(by=['year', 'month', 'usage_count'], ascending=[True, True, False])

# Filter out the last month of the last year
last_year = df_combined['year'].max()
last_month = df_combined[df_combined['year'] == last_year]['month'].max()
df_combined = df_combined[~((df_combined['year'] == last_year) & (df_combined['month'] == last_month))]

# Display the combined DataFrame
print("df combined:")
print(df_combined)

# Convert the DataFrame to a list of dictionaries
records = df_combined.to_dict('records')

# Create or get the 'popular_stations' collection
popular_stations_collection = db['popular_stations']

# Insert the records into the new collection
result = popular_stations_collection.insert_many(records)

print(f"Inserted {len(result.inserted_ids)} documents into the 'popular_stations' collection.")

# Optionally, you can create an index on the location field for geospatial queries
popular_stations_collection.create_index([("location", "2dsphere")])

print("Created a 2dsphere index on the 'location' field.")

# Close the MongoDB connection
client.close()