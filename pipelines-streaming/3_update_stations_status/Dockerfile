FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY update_status.py /opt/update_status.py

# Set the working directory
WORKDIR /opt

# Copy the Kafka clients jar
COPY kafka-clients-3.5.1.jar /opt/spark/jars/kafka-clients-3.5.1.jar

# Set the entrypoint to run specific test:
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.neo4j:neo4j-connector-apache-spark_2.12:5.2.0_for_spark_3", \
            "--repositories", "https://repo1.maven.org/maven2/", \
            "/opt/update_status.py"]

