from kafka import KafkaProducer
import urllib.request
import time
import logging

# Configurar el logging
logging.basicConfig(level=logging.INFO)
'''
# Configuración del Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: str(v).encode('utf-8')
)


def send_to_kafka(topic, data):
    """Envia los datos al tópico de Kafka"""
    try:
        producer.send(topic, value=data)
        producer.flush()
        logging.info(f"Datos enviados a Kafka: {data[:50]}...")  # Log solo los primeros 50 caracteres
    except Exception as e:
        logging.error(f"Error al enviar datos a Kafka: {e}")
'''
def fetch_data():
    """Descarga los datos desde la API"""
    try:
        url = 'https://opendata-ajuntament.barcelona.cat/data/ca/dataset/estat-estacions-bicing/resource/1b215493-9e63-4a12-8980-2d7e0fa19f85/download/Estat_Estacions_Bicing_securitzat_json.json'
        request = urllib.request.Request(url)
        request.add_header('Authorization', 'b8092b37b02cda27d5d8e56cde9bfa9a49b15dab99bb06a06e89f72a931fa644')
        with urllib.request.urlopen(request) as response:
            data = response.read().decode('utf-8')
        return data
    except Exception as e:
        logging.error(f"Error al descargar datos: {e}")
        return None

# Bucle principal para descargar datos y enviarlos a Kafka
kafka_topic = "estat_estacions"

for i in range(6):
    data = fetch_data()
    if data:
        #send_to_kafka(kafka_topic, data)
        # Muestra los datos por pantalla
        logging.info(f"Datos descargados: {data[:500]}...")  # Mostrar los primeros 500 caracteres
        logging.info(f"Datos descargados y mostrados por pantalla.")  # Log de confirmación


    # Esperar 10 segundos antes de la siguiente solicitud
    time.sleep(10)