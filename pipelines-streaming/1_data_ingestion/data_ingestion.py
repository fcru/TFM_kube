from kafka import KafkaProducer
import urllib.request
import time
import json
import os
import logging
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
import psutil

# Configurar Pushgateway y métricas
registry = CollectorRegistry()

# Métricas
message_counter = Counter('data_ingestion_job_kafka_messages_enviados', 'Total number of messages sent to Kafka', registry=registry)
error_counter = Counter('data_ingestion_job_kafka_errors_total', 'Total number of errors occurred', registry=registry)
iteration_duration = Gauge('data_ingestion_job_kafka_iteration_duration_seconds', 'Duration of each Kafka send iteration', registry=registry)
fetch_data_duration = Gauge('data_ingestion_job_api_fetch_data_duration_seconds', 'Duration of fetching data from the API', registry=registry)
cpu_usage_gauge = Gauge('data_ingestion_job_cpu_usage_percent', 'CPU usage of the process', registry=registry)
memory_usage_gauge = Gauge('data_ingestion_job_memory_usage_bytes', 'Memory usage of the process', registry=registry)

# Download the data from the API
def fetch_data():
    start_time = time.time()  # Inicia la medición del tiempo de la API
    try:
        url = 'https://opendata-ajuntament.barcelona.cat/data/ca/dataset/estat-estacions-bicing/resource/1b215493-9e63-4a12-8980-2d7e0fa19f85/download/Estat_Estacions_Bicing_securitzat_json.json'
        request = urllib.request.Request(url)
        request.add_header('Authorization', 'b8092b37b02cda27d5d8e56cde9bfa9a49b15dab99bb06a06e89f72a931fa644')
        with urllib.request.urlopen(request) as response:
            data = response.read().decode('utf-8')

        # Calcula y establece la duración de la llamada a la API
        fetch_data_duration.set(time.time() - start_time)
        return data
    except Exception as e:
        logging.error(f"Error al descargar datos: {e}")
        error_counter.inc()  # Incrementa el contador de errores
        return None

kafka_topic = "estat_estacions"

# Set the path to the jaas.conf file
jaas_file_path = '/etc/kafka_client_jaas.conf'

# Set the system configuration property for the jaas.conf file
os.environ['KAFKA_OPTS'] = f'-Djava.security.auth.login.config={jaas_file_path}'

for i in range(6):
    start_time = time.time()

    # Configure the Kafka Producer with SASL/PLAIN
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        security_protocol='SASL_PLAINTEXT',  # Protocolo de seguridad
        sasl_mechanism='PLAIN',              # Mecanismo de SASL
        sasl_plain_username='user1',         # Usuario SASL (mismo que en jaas.conf)
        sasl_plain_password='LtG5496WgU',    # Contraseña SASL (mismo que en jaas.conf)
        key_serializer=lambda k: str(k).encode('utf-8'),  # Serializar la clave como string UTF-8
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializar el valor como JSON UTF-8
    )

    data = fetch_data()
    if data:
        json_obj = json.loads(data)
        stations = json_obj["data"]["stations"]
        for station in stations:
            key = station["station_id"]
            # Send the key-value pair to the Kafka topic
            try:
                producer.send(topic=kafka_topic, key=key, value=station)
                message_counter.inc()
            except Exception as e:
                logging.error(f"Error al enviar mensaje a Kafka: {e}")
                error_counter.inc()
            producer.flush()  # Asegura que el mensaje se envía inmediatamente

    producer.close()

    # Calcula la duración de la iteración
    iteration_duration.set(time.time() - start_time)

    # Actualiza el uso de CPU y memoria
    process = psutil.Process()
    cpu_usage = process.cpu_percent()
    memory_usage = process.memory_info().rss
    cpu_usage_gauge.set(cpu_usage)
    memory_usage_gauge.set(memory_usage)

    # Push metrics to Pushgateway
    push_to_gateway('pushgateway:9091', job='data_ingestion', registry=registry)

    # Wait 10 seconds before the next request
    time.sleep(10)
