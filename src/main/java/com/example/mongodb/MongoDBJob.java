package com.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MongoDBJob {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://mongodb:27017");

        // Access the database
        MongoDatabase database = mongoClient.getDatabase("testdb");

        // Access the collection
        MongoCollection<Document> collection = database.getCollection("uuids");

        // Generate 10 random UUIDs
        List<Document> uuidDocuments = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID uuid = UUID.randomUUID();
            Document doc = new Document("uuid", uuid.toString());
            uuidDocuments.add(doc);
        }

        // Insert UUIDs into MongoDB collection
        collection.insertMany(uuidDocuments);

        // Close the MongoDB connection
        mongoClient.close();
    }
}
