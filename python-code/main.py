from pymongo import MongoClient
import uuid

def main():

    mongo_client = MongoClient("mongodb://mongodb:27017")

    # Access the database
    database = mongo_client["testdb"]

    # Access the collection
    collection = database["uuids"]

    # Generate 10 random UUIDs
    uuid_documents = [{"uuid": str(uuid.uuid4())} for _ in range(10)]
    print(uuid_documents)
    # Insert UUIDs into MongoDB collection
    collection.insert_many(uuid_documents)

    # Close the MongoDB connection
    mongo_client.close()

if __name__ == "__main__":
    main()