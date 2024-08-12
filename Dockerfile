FROM spark:3.4.1-scala2.12-java11-ubuntu

USER root

COPY target/spark-job-jar-with-dependencies.jar /opt/spark-job.jar

# Set the working directory
WORKDIR /opt

# Set the entrypoint to run specific test:
# ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "com.example.spark.SparkJob", "/opt/spark-job.jar"]
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "com.example.spark.SparkStreamingJob", "/opt/spark-job.jar"]
# ENTRYPOINT ["java", "-cp", "/opt/spark-job.jar", "com.example.mongodb.MongoDBJob"]
