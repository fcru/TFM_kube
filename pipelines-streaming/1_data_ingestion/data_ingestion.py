from kafka import KafkaProducer
import urllib.request
import time
import json
import os
import logging
from prometheus_client import start_http_server, Counter, Histogram

# Start server to expose metrics on port 8000
start_http_server(8000)

# Definir las métricas
messages_sent = Counter('kafka_messages_sent_total', 'Total de mensajes enviados a Kafka')
api_request_latency = Histogram('api_request_latency_seconds', 'Latencia en la solicitud a la API')
processing_time = Histogram('processing_time_seconds', 'Tiempo de procesamiento del ciclo completo')
errors_total = Counter('errors_total', 'Errores ocurridos durante la ejecución')

def fetch_data():
    """Descargar los datos desde la API"""
    try:
        url = 'https://opendata-ajuntament.barcelona.cat/data/ca/dataset/estat-estacions-bicing/resource/1b215493-9e63-4a12-8980-2d7e0fa19f85/download/Estat_Estacions_Bicing_securitzat_json.json'
        request = urllib.request.Request(url)
        request.add_header('Authorization', 'b8092b37b02cda27d5d8e56cde9bfa9a49b15dab99bb06a06e89f72a931fa644')

        start_time = time.time()  # Iniciar temporizador para medir latencia de la API
        with urllib.request.urlopen(request) as response:
            data = response.read().decode('utf-8')
        api_request_latency.observe(time.time() - start_time)  # Medir y registrar latencia de la API

        return data
    except Exception as e:
        errors_total.inc()  # Incrementar contador de errores
        logging.error(f"Error al descargar datos: {e}")
        return None

kafka_topic = "estat_estacions"

# Set the path to the jaas.conf file
jaas_file_path = '/etc/kafka_client_jaas.conf'

# Set the system configuration property for the jaas.conf file
os.environ['KAFKA_OPTS'] = f'-Djava.security.auth.login.config={jaas_file_path}'

for i in range(6):
    # Measure the total processing time of each cycle
    with processing_time.time():
        # Configure the Kafka Producer with SASL/PLAIN
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username='user1',
            sasl_plain_password='fY6mjamEAH',
            key_serializer=lambda k: str(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        data = fetch_data()
        if data:
            json_obj = json.loads(data)
            stations = json_obj["data"]["stations"]
            for station in stations:
                key = station["station_id"]
                # Send the key-value pair to the Kafka topic
                producer.send(topic=kafka_topic, key=key, value=station)
                messages_sent.inc()  # Incrementar el contador de mensajes enviados
                producer.flush()  # Asegura que el mensaje se envía inmediatamente

        producer.close()
        # Esperar 10 segundos antes de la próxima solicitud
        time.sleep(10)
