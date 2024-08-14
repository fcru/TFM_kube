package com.example.spark;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

public class SparkStreamingJob {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Create a temporary JAAS configuration file
        File jaasFile = File.createTempFile("kafka_client_jaas", ".conf");
        try (FileWriter writer = new FileWriter(jaasFile)) {
            writer.write("KafkaClient {\n");
            writer.write("  org.apache.kafka.common.security.plain.PlainLoginModule required\n");
            writer.write("  username=\"user1\"\n");
            writer.write("  password=\"11tC2mApeB\";\n");
            writer.write("};\n");
        }

        // Apply the JAAS configuration
        System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());

        // Set up Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("SparkStreamingKafkaExample")
                .setMaster("local[*]");

        // Set up Spark Streaming context with a 3-second batch interval
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-streaming-group");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT"); // Or "SASL_SSL" if SSL is used
        kafkaParams.put("sasl.mechanism", "PLAIN"); // Change if using different SASL mechanism

        // Create a DStream from Kafka
        JavaDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList("my-topic"), kafkaParams)
        );

        // Process each RDD in the DStream
        stream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                System.out.printf("Received message: (key: %s, value: %s)%n", record.key(), record.value());
            });
        });

        // Start the streaming context and await termination
        ssc.start();
        ssc.awaitTermination();
    }
}
