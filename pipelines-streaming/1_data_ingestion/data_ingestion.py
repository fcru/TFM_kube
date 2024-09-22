from kafka import KafkaProducer
import urllib.request
import time
import json
import os
import logging

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

kafka_topic = "estat_estacions"

# Establecer la ruta al archivo jaas.conf
jaas_file_path = '/etc/kafka_client_jaas.conf'

# Establecer la propiedad de configuración del sistema para el archivo jaas.conf
os.environ['KAFKA_OPTS'] = f'-Djava.security.auth.login.config={jaas_file_path}'

for i in range(6):
    # Configurar el Kafka Producer con SASL/PLAIN
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        security_protocol='SASL_PLAINTEXT',  # Protocolo de seguridad
        sasl_mechanism='PLAIN',              # Mecanismo de SASL
        sasl_plain_username='user1',         # Usuario SASL (mismo que en jaas.conf)
        sasl_plain_password='Ig9D1s4pRq',    # Contraseña SASL (mismo que en jaas.conf)
        key_serializer=lambda k: str(k).encode('utf-8'),  # Serializar la clave como string UTF-8
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializar el valor como JSON UTF-8
    )

    data = fetch_data()
    if data:
        json_obj = json.loads(data)
        stations = json_obj["data"]["stations"]
        for station in stations:
            # Acceder a los campos de cada estación
            key = station["station_id"]
            #value = json.dumps(station_status) # No se debe hacer el dumps
            # Enviar el par clave-valor al tópico de Kafka
            producer.send(topic=kafka_topic, key=key, value=station)
            producer.flush()  # Asegura que el mensaje se envía inmediatamente

    producer.close()
    # Esperar 10 segundos antes de la siguiente solicitud
    time.sleep(10)
