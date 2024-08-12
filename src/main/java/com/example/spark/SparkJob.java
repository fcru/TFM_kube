package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SparkJob {

    public static String getTimePartition(long timestamp) {
        // Create a Date object from the timestamp
        Date date = new Date(timestamp);

        // Define the desired format
        SimpleDateFormat sdf = new SimpleDateFormat("'year='yyyy/'month='MM/'day='dd/'hour='HH");

        // Format the Date object into the desired format and return the string
        return sdf.format(date);
    }

    public static void main(String[] args) {
        // Create a Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Save UUIDs to HDFS")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Generate 10 random UUIDs
        List<String> uuidList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            uuidList.add(UUID.randomUUID().toString());
        }

        // Parallelize the UUID list to create an RDD
        JavaRDD<String> uuidRdd = sc.parallelize(uuidList);

        Long executionTime = new Long(System.currentTimeMillis());
        String timePartition = getTimePartition(executionTime);

        uuidRdd.saveAsTextFile("hdfs://hadooop-hadoop-hdfs-nn:9000/spark-job/" + timePartition + "/" + executionTime);

        // Stop the Spark context
        sc.stop();
    }
}
