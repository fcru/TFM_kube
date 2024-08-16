from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
import uuid

def main():
    print("executing script")
    mongo_client = MongoClient("mongodb://mongodb:27017")
    print("connection done!")
    # Access the database
    database = mongo_client["testdb"]

    # Access the collection
    collection = database.get_collection('uuids', write_concern=WriteConcern(w=1))
    uuid_documents = [{"uuid": str(uuid.uuid4())} for _ in range(10)]
    print(uuid_documents)
    # Insert UUIDs into MongoDB collection
    result = collection.insert_many(uuid_documents)

    print(f"Inserted {len(result.inserted_ids)} documents")

    # Close the MongoDB connection
    mongo_client.close()

if __name__ == "__main__":
    main()