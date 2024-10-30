import streamlit as st
import pandas as pd
import json
from confluent_kafka import Consumer, KafkaError
import pydeck as pdk
import time

# Configuración del consumidor Kafka
conf = {
    'bootstrap.servers': "kafka:9092",
    'group.id': "streamlit-group-" + str(hash(st)),  # Grupo único
    'auto.offset.reset': 'latest',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'user1',
    'sasl.password': 'LtG5496WgU'
}

consumer = Consumer(conf)
consumer.subscribe(['truck-route'])

# Colores asignados a los camiones
camion_colores = {
    i: color for i, color in enumerate([
        [255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 0], [255, 165, 0], [128, 0, 128],
        [0, 255, 255], [255, 192, 203], [255, 20, 147], [75, 0, 130], [139, 69, 19], [0, 100, 0],
        [173, 255, 47], [255, 69, 0], [0, 0, 128], [186, 85, 211], [47, 79, 79], [154, 205, 50],
        [255, 140, 0], [70, 130, 180], [72, 61, 139], [100, 149, 237], [255, 105, 180],
        [0, 191, 255], [34, 139, 34]
    ])
}

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

# Función para procesar los mensajes y extraer datos
def procesar_rutas(mensajes):
    data = []
    lineas = []
    camion_info = {}
    for mensaje in mensajes:
        try:
            # Decodificar el mensaje JSON
            mensaje_decodificado = json.loads(mensaje)
            truck_id = mensaje_decodificado["truck_id"]
            distancia_optima = mensaje_decodificado.get("distance", 0)
            balance_bicicletas = mensaje_decodificado.get("balance_bicicletas", 0)

            # Obtener el color del camión
            color_truck = camion_colores.get(truck_id, [255, 255, 255])

            # Acceder a la lista de estaciones dentro de "optimal_route"
            estaciones = mensaje_decodificado.get("optimal_route", [])
            coordenadas_ruta = []

            # Extraer datos para cada estación
            for idx, estacion in enumerate(estaciones):
                lat = estacion["lat"]
                lon = estacion["lon"]
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

            # Agregar información a la tabla de camiones
            camion_info[truck_id] = {
                "ID Camión": truck_id,
                "Color": color_truck,
                "Distancia Óptima (metros)": f"{distancia_optima:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                "Balance Bicicletas": balance_bicicletas
            }

        except json.JSONDecodeError:
            st.error("Error de decodificación JSON: " + mensaje)
        except KeyError as e:
            st.error(f"Error: clave faltante {str(e)} en el mensaje: {mensaje}")
    return pd.DataFrame(data), lineas, camion_info

# Interfaz de Streamlit
st.title("Rutas óptimas para reabastecimiento de bicicletas")
st.subheader("Rutas recibidas desde Kafka:")

map_placeholder = st.empty()
table_placeholder = st.empty()

while True:
    rutas = consumir_mensajes()

    if rutas:
        df_rutas, rutas_lineas, camion_info = procesar_rutas(rutas)

        # Configurar pydeck con los datos y las líneas
        if not df_rutas.empty:
            # Definir el layer de las estaciones (puntos) con colores por camión
            layer_puntos = pdk.Layer(
                "ScatterplotLayer",
                data=df_rutas,
                get_position='[lon, lat]',
                get_radius=90,
                get_color='color',
                pickable=True
            )

            # Definir el layer de las líneas que conectan las estaciones, con colores
            layer_lineas = pdk.Layer(
                "PathLayer",
                data=rutas_lineas,
                get_path="path",
                get_width=5,
                get_color="color",
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
            map_placeholder.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v9',
                initial_view_state=vista_inicial,
                layers=[layer_puntos, layer_lineas],
                tooltip={
                    "html": "<b>ID Estación:</b> {ID estación}<br><b>Capacidad:</b> {Capacidad}<br><b>Status:</b> {Status}<br><b>Bicicletas disponibles:</b> {Bicicletas disponibles}<br><b>Orden:</b> {orden}<br><b>Camión:</b> {Camión}",
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            ))

            # Crear y actualizar la tabla de información de los camiones
            df_camiones = pd.DataFrame.from_dict(camion_info, orient="index").reset_index(drop=True)
            # Modificar la columna 'Color' para que sea una celda HTML con el color de fondo
            df_camiones['Color'] = df_camiones['Color'].apply(
                lambda rgb: f'<div style="background-color: rgb({rgb[0]}, {rgb[1]}, {rgb[2]}); '
                            f'width: 40px; height: 20px; border-radius: 5px;"></div>'
            )
            # Mostrar la tabla usando HTML para incluir las celdas de color
            table_placeholder.write(df_camiones.to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.write("No se han recibido rutas nuevas.")

    # Espera 10 segundos antes de volver a consultar Kafka
    time.sleep(10)

