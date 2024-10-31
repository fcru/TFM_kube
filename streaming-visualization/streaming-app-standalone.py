import streamlit as st
import pandas as pd
import json
import pydeck as pdk
import time

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

# Función para procesar un bloque de rutas
def procesar_rutas(mensajes):
    data = []
    lineas = []
    camion_info = {}
    for mensaje in mensajes:
        try:
            # Obtener datos del mensaje JSON
            truck_id = mensaje["truck_id"]
            distancia_optima = mensaje.get("distance", 0)
            balance_bicicletas = mensaje.get("balance_bicicletas", 0)
            color_truck = camion_colores.get(truck_id, [255, 255, 255])
            estaciones = mensaje.get("optimal_route", [])
            coordenadas_ruta = []

            # Extraer datos de cada estación
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

            # Añadir línea de coordenadas
            if len(coordenadas_ruta) > 1:
                lineas.append({"path": coordenadas_ruta, "color": color_truck})

            # Agregar información a la tabla de camiones
            camion_info[truck_id] = {
                "ID Camión": truck_id,
                "Color": color_truck,
                "Distancia Óptima (metros)": f"{distancia_optima:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                "Bicicletas a Reponer": balance_bicicletas
            }

        except KeyError as e:
            st.error(f"Error: clave faltante {str(e)} en el mensaje")
    return pd.DataFrame(data), lineas, camion_info

# Interfaz de Streamlit
st.title("Rutas óptimas para reabastecimiento de bicicletas")

map_placeholder = st.empty()
table_placeholder = st.empty()

# Repetir el proceso 10 veces
for _ in range(10):
    # Cargar los datos desde el archivo JSON
    try:
        with open('optimal_route.json', 'r') as f:
            rutas_totales = json.load(f)
    except FileNotFoundError:
        st.error("Archivo 'optimal_route.json' no encontrado.")
        rutas_totales = []

    # Iterar sobre bloques de rutas en el JSON con una pausa de 5 segundos
    for bloque_rutas in rutas_totales:
        if not bloque_rutas:
            st.write("No se han encontrado rutas nuevas en el bloque.")
            continue

        df_rutas, rutas_lineas, camion_info = procesar_rutas(bloque_rutas)

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
            df_camiones['Color'] = df_camiones['Color'].apply(
                lambda rgb: f'<div style="background-color: rgb({rgb[0]}, {rgb[1]}, {rgb[2]}); '
                            f'width: 40px; height: 20px; border-radius: 5px;"></div>'
            )
            # Mostrar la tabla usando HTML para incluir las celdas de color
            table_placeholder.write(df_camiones.to_html(escape=False, index=False), unsafe_allow_html=True)
        else:
            st.write("No hay rutas en este bloque para visualizar.")

        # Pausa de 5 segundos antes de procesar el siguiente bloque
        time.sleep(5)

    # Espera 5 segundos antes de volver a procesar todos los bloques del JSON
    time.sleep(5)
