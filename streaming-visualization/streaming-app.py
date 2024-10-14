import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer, KafkaError
import pydeck as pdk
import random
import time

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': "kafka:9092",
    'group.id': "streamlit-group-" + str(hash(st)),  # Grupo único
    'auto.offset.reset': 'latest',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'user1',
    'sasl.password': 'fY6mjamEAH'
}

consumer = Consumer(conf)
consumer.subscribe(['truck-route'])

# Función para obtener mensajes de Kafka
def consumir_mensajes():
    mensajes = []
    timeout = 20.0
    while True:
        msg = consumer.poll(timeout)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                st.error(f"Error de Kafka: {msg.error()}")
                break
        else:
            mensajes.append(msg.value().decode('utf-8'))
    return mensajes

# Generar un color aleatorio
def generar_color_aleatorio():
    return [random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)]

# Función para procesar los mensajes y extraer las coordenadas
def procesar_rutas(mensajes):
    data = []
    lineas = []
    colores = {}
    for mensaje in mensajes:
        try:
            # Decodificar el mensaje JSON
            mensaje_decodificado = json.loads(mensaje)
            truck_id = mensaje_decodificado["truck_id"]

            # Generar un color único para cada camión
            if truck_id not in colores:
                colores[truck_id] = generar_color_aleatorio()

            color_truck = colores[truck_id]

            # Acceder a la lista de estaciones dentro de "optimal_route"
            estaciones = mensaje_decodificado.get("optimal_route", [])
            coordenadas_ruta = []

            for idx, estacion in enumerate(estaciones):
                lat = estacion["lat"]
                lon = estacion["lon"]
                # Añadir cada estación con su color correspondiente
                data.append({
                    "lat": lat,
                    "lon": lon,
                    "color": color_truck,
                    "orden": str(idx + 1),
                    "ID estación": estacion.get("station_id", "N/A"),
                    "Capacidad": estacion.get("capacity", "N/A"),
                    "Status": estacion.get("check_status", "N/A"),
                    "Bicicletas disponibles": estacion.get("num_bikes_available", "N/A"),
                    "Camión": estacion.get("truck", "N/A")
                })
                coordenadas_ruta.append([lon, lat])

            # Añadir las coordenadas como una línea
            if len(coordenadas_ruta) > 1:
                lineas.append({"path": coordenadas_ruta, "color": color_truck})

        except json.JSONDecodeError:
            st.error("Error de decodificación JSON: " + mensaje)
        except KeyError as e:
            st.error(f"Error: clave faltante {str(e)} en el mensaje: {mensaje}")
    return pd.DataFrame(data), lineas

# Interfaz de Streamlit
st.title("Visualización de rutas óptimas para reabastecimiento de bicicletas")

st.subheader("Rutas recibidas desde Kafka:")

placeholder = st.empty()  # Contenedor para la visualización dinámica

while True:
    rutas = consumir_mensajes()

    if rutas:
        df_rutas, rutas_lineas = procesar_rutas(rutas)

        # Configurar pydeck con los datos y las líneas
        if not df_rutas.empty:
            # Definir el layer de las estaciones (puntos) con colores por camión
            layer_puntos = pdk.Layer(
                "ScatterplotLayer",
                data=df_rutas,
                get_position='[lon, lat]',
                get_radius=90,
                get_color='color',  # Asignar color único para cada camión
                pickable=True
            )

            # Definir el layer de las líneas que conectan las estaciones, con colores
            layer_lineas = pdk.Layer(
                "PathLayer",
                data=rutas_lineas,
                get_path="path",
                get_width=5,
                get_color="color",  # Asignar color único para cada camión
                width_min_pixels=3,
            )

            # Definir la vista inicial del mapa
            vista_inicial = pdk.ViewState(
                latitude=df_rutas["lat"].mean(),
                longitude=df_rutas["lon"].mean(),
                zoom=12,
                pitch=40,
            )

            # Crear el mapa con pydeck, incluyendo tooltip
            placeholder.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v9',
                initial_view_state=vista_inicial,
                layers=[layer_puntos, layer_lineas],
                tooltip={
                    "html": "<b>ID Estación:</b> {ID estación}<br><b>Capacidad:</b> {Capacidad}<br><b>Status:</b> {Status}<br><b>Bicicletas disponibles:</b> {Bicicletas disponibles}<br><b>Orden:</b> {orden}<br><b>Camión:</b> {Camión}",
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            ))
    else:
        st.write("No se han recibido rutas nuevas.")

    # Espera 10 segundos antes de volver a consultar Kafka
    time.sleep(20)


