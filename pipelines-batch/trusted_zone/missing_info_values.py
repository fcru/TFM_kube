from collections import defaultdict
from datetime import datetime

from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://mongodb:27017/')
db = client['bicing_db']
collection = db['estacio']


def drop_station_id(station_id):
    """Remove documents with the specified station_id."""
    result = collection.delete_many({"station_id": station_id})
    print(f"Deleted {result.deleted_count} documents with station_id: {station_id}")

    # Find and drop duplicates based on station_id and date
    duplicates = collection.aggregate([
        {"$group": {
            "_id": {"station_id": "$station_id", "date": "$date"},
            "count": {"$sum": 1},
            "docs": {"$push": "$_id"}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ], allowDiskUse=True)  # Enable disk use for the aggregation

    for duplicate in duplicates:
        # Keep the first document and remove the rest
        ids_to_delete = duplicate["docs"][1:]  # Keep the first document
        collection.delete_many({"_id": {"$in": ids_to_delete}})
        print(
            f"Deleted {len(ids_to_delete)} duplicate documents with station_id: {duplicate['_id']['station_id']} and date: {duplicate['_id']['date']}")


def generate_null_values_count_report():
    pipeline = [
        {
            "$addFields": {
                "year": {"$year": {"$toDate": "$date"}},    # Extract the year from the date field
                "month": {"$month": {"$toDate": "$date"}}   # Extract the month from the date field
            }
        },
        {
            "$project": {
                "station_id": 1,
                "year": 1,
                "month": 1,
                "has_null": {
                    "$or": [
                        {"$eq": ["$name", None]},
                        {"$eq": ["$name", ""]},
                        {"$eq": ["$capacity", None]},
                        {"$eq": ["$geometry", None]},
                        {"$eq": ["$altitude", None]},
                        {"$eq": ["$address", None]},
                        {"$eq": ["$address", ""]},
                        {"$eq": ["$post_code", None]}
                    ]
                }
            }
        },
        {
            "$match": {
                "has_null": True  # Match documents that have any null values
            }
        },
        {
            "$group": {
                "_id": {
                    "station_id": "$station_id",
                    "year": "$year",
                    "month": "$month"
                },
                "count": {"$sum": 1}  # Count the number of documents with null values per station, year, and month
            }
        },
        {
            "$sort": {
                "_id.station_id": 1,  # Sort by station_id, year, and month
                "_id.year": 1,
                "_id.month": 1
            }
        }
    ]

    return list(collection.aggregate(pipeline))



def update_null_values():
    pipeline = [
        {
            "$sort": {"date": 1}  # Sort by the date field to get the earliest non-null values
        },
        {
            "$group": {
                "_id": "$station_id",  # Group only by station_id
                "firstNonNull": {
                    "$first": {
                        "name": {"$cond": [{"$ne": ["$name", None]}, "$name", "$$REMOVE"]},
                        "capacity": {"$cond": [{"$ne": ["$capacity", None]}, "$capacity", "$$REMOVE"]},
                        "geometry": {"$cond": [{"$ne": ["$geometry", None]}, "$geometry", "$$REMOVE"]},
                        "altitude": {"$cond": [{"$ne": ["$altitude", None]}, "$altitude", "$$REMOVE"]},
                        "address": {"$cond": [{"$ne": ["$address", None]}, "$address", "$$REMOVE"]},
                        "post_code": {"$cond": [{"$ne": ["$post_code", None]}, "$post_code", "$$REMOVE"]}
                    }
                }
            }
        },
        {
            "$project": {
                "station_id": "$_id",  # Return only the station_id
                "firstNonNull": 1      # Return the first non-null values
            }
        }
    ]

    # Run the pipeline with allowDiskUse=True to allow external sorting
    results = collection.aggregate(pipeline, allowDiskUse=True)

    # Update documents with non-null values
    for result in results:
        station_id = result["station_id"]
        updates = result["firstNonNull"]

        # If updates is not empty, perform the update
        if updates:
            collection.update_many(
                {"station_id": station_id},
                {"$set": updates}
            )
        else:
            # Print the station_id when no updates are found
            print(f"No updates for station_id: {station_id}")
            print("Dropping station_id...")
            drop_station_id(station_id)
            print("Dropped")

def convert_to_integer_or_date():
    # Step 1: Find documents where station_id, num_bikes_available, num_docks_available are strings
    # Also find documents where the date is in string format
    query = {
        "$or": [
            {"station_id": {"$type": "string"}},
            {"status.num_bikes_available": {"$type": "string"}},
            {"status.num_docks_available": {"$type": "string"}},
            {"date": {"$type": "string"}}
        ]
    }

    # Step 2: Iterate over the documents and convert the fields to integers and dates
    documents = collection.find(query)

    for doc in documents:
        # Convert station_id if it's a string
        if isinstance(doc['station_id'], str):
            doc['station_id'] = int(doc['station_id'])

        # Convert num_bikes_available and num_docks_available in the status array
        for status in doc['status']:
            if isinstance(status['num_bikes_available'], str):
                status['num_bikes_available'] = int(status['num_bikes_available'])
            if isinstance(status['num_docks_available'], str):
                status['num_docks_available'] = int(status['num_docks_available'])

        # Convert date from string to ISODate format
        if isinstance(doc.get('date'), str):
            try:
                # Parse date string and convert to datetime.datetime object
                doc['date'] = datetime.strptime(doc['date'], '%Y-%m-%d')
            except ValueError:
                print(f"Date format error for document with _id {doc['_id']}: {doc['date']}")
                continue  # Skip this document if the date format is incorrect

        # Step 3: Update the document with the converted values
        collection.update_one({"_id": doc['_id']}, {"$set": doc})

    print("Conversion completed!")


# Main execution
if __name__ == "__main__":
    # Generate and print null values report
    print("converting in integer..")
    convert_to_integer_or_date()
    print("Converted!")
    null_values_count = generate_null_values_count_report()
    # Organize the data by station_id
    station_data = defaultdict(lambda: defaultdict(dict))
    for item in null_values_count:
        station_id = item['_id']['station_id']
        year = item['_id']['year']
        month = item['_id']['month']
        count = item['count']
        station_data[station_id][year][month] = count

    # Print the report
    print("Null Values Count Report:")
    print("-------------------------")
    for station_id, years in station_data.items():
        print(f"Station ID: {station_id}")
        for year, months in years.items():
            print(f"  Year {year}:")
            for month, count in months.items():
                print(f"    Month {month}: {count}")
        print()

    # Update documents with non-null values
    print("Updating documents with non-null values...")
    update_null_values()
    print("Documents updated successfully.")


# Close the MongoDB connection
client.close()