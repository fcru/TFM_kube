FROM spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY data_ingestion.py /opt/data_ingestion.py

# Exponer el puerto de métricas
EXPOSE 8000

# Set the working directory
WORKDIR /opt

COPY kafka_client_jaas.conf /etc/kafka_client_jaas.conf

# Set the entrypoint to run specific test:
ENTRYPOINT ["python3", "/opt/data_ingestion.py"]
