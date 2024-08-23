FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Spark-Job Example
#COPY ./src/main/example/spark-job.py /opt/spark-job.py

# MongoDB Example
#COPY ./src/main/example/mongo-job.py /opt/mongo-job.py

# Streaming job
#COPY ./src/main/example/streaming-job.py /opt/streaming-job.py

#Batch Scrape Ajuntament
COPY ./src/main/ /opt/

# Set the working directory
WORKDIR /opt

# Commands to run kafka
#COPY kafka-clients-3.5.1.jar /opt/spark/jars/kafka-clients-3.5.1.jar
#COPY kafka_client_jaas.conf /etc/kafka_client_jaas.conf
#ENV JAVA_TOOL_OPTIONS="-Djava.security.auth.login.config=/etc/kafka_client_jaas.conf"

# Set the entrypoint to run specific test:
#ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark-job.py"]
#ENTRYPOINT ["python3", "/opt/mongo-job.py"]
#ENTRYPOINT ["python3", "/opt/landing-zone/batch/batch_scrape_bicing.py"]
#ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", \
#            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.1.1", \
#            "/opt/streaming-job.py"]
ENTRYPOINT ["sleep", "infinity"]