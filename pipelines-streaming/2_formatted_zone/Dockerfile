FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY read_kafka.py /opt/read_kafka.py

# Set the working directory
WORKDIR /opt

COPY kafka-clients-3.5.1.jar /opt/spark/jars/kafka-clients-3.5.1.jar

# Set the entrypoint to run specific test:
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", "/opt/read_kafka.py"]
