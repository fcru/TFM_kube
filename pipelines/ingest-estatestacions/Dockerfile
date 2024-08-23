FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_ingestion.py /opt/data_ingestion.py

# Set the working directory
WORKDIR /opt

COPY kafka-clients-3.5.1.jar /opt/spark/jars/kafka-clients-3.5.1.jar
COPY kafka_client_jaas.conf /etc/kafka_client_jaas.conf

# Set the entrypoint to run specific test:
ENTRYPOINT ["python3", "/opt/data_ingestion.py"]
