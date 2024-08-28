FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

ENV SPARK_EXECUTOR_MEMORY=4g
ENV SPARK_DRIVER_MEMORY=4g

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

#Batch Scrape Ajuntament
COPY . /opt/

# Set the working directory
WORKDIR /opt


RUN curl -L -o /opt/spark/jars/mongo-spark-connector_2.12-10.1.1.jar \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar && \
    curl -L -o /opt/spark/jars/mongodb-driver-sync-4.8.2.jar \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar && \
    curl -L -o /opt/spark/jars/mongodb-driver-core-4.8.2.jar \
    https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar && \
    curl -L -o /opt/spark/jars/bson-4.8.2.jar \
    https://repo1.maven.org/maven2/org/mongodb/bson/4.8.2/bson-4.8.2.jar && \
    curl -L -o /opt/spark/jars/bson-record-codec-4.8.2.jar \
    https://repo1.maven.org/maven2/org/mongodb/bson-record-codec/4.8.2/bson-record-codec-4.8.2.jar

ENV PACKAGES=org.mongodb.spark:mongo-spark-connector_2.12:10.1.1


# Set the entrypoint to run specific test:
#ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark-job.py"]
#ENTRYPOINT ["python3", "/opt/mongo-job.py"]
#ENTRYPOINT ["python3", "/opt/landing-zone/batch/batch_scrape_bicing.py"]
#ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", \
            #"org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1", \
            #"/opt/streaming-job.py"]
ENTRYPOINT ["sleep", "infinity"]