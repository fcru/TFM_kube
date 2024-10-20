import pandas as pd
import os
from datetime import datetime, timedelta
import altair as alt
import streamlit as st
import folium
import streamlit.components.v1
from folium import plugins
from streamlit_folium import folium_static
from streamlit_option_menu import option_menu
from functions import *

def get_mapa_bcn():
    # Mapa centrado en Barcelona
    bcn_map = folium.Map(location=[41.3851, 2.1764], zoom_start=13)

    folium.plugins.Fullscreen(
        position="topright",
        title="Pantalla completa",
        title_cancel="Salir",
        force_separate_button=True,
    ).add_to(bcn_map)

    return bcn_map

def get_tooltip_eficiencia(row):
    text = "<span style='font-family:Arial;'> " +\
            f"<b>ID estación</b>: {row['station_id']}<br>" +\
            f"<b>Nombre</b>: {capitalizar_nombre(row['name'])}<br>" +\
            f"<b>Eficiencia</b>: {row['tasa_eficiencia_ajustada']:.2f} </span>"
    return text

def get_tooltip_demanda(row):
    text = "<span style='font-family:Arial;'> " +\
            f"<b>ID estación</b>: {row['station_id']}<br>" +\
            f"<b>Nombre</b>: {capitalizar_nombre(row['name'])}<br>" +\
            f"<b>Demanda total</b>: {row['rotacion']:.2f} </span>"
    return text


def crear_grafico_comparativo(real_data, pred_data, type):
    if type == 'taken':
        real_bikes = real_data['total_bikes_taken']
        title = "Total bicicletas tomadas"
    elif type == 'returned':
        real_bikes = real_data['total_bikes_returned']
        title = "Total bicicletas devueltas"

    df_real = pd.DataFrame({
        'hora': real_data['fecha'],
        'bikes': real_bikes,
        'type': 'Real'
    }).reset_index(drop=True)

    df_pred = pd.DataFrame({
        'hora': real_data['fecha'],
        'bikes': pred_data['Predicted'],
        'type': 'Predicción'
    }).reset_index(drop=True)

    df_interval = pd.DataFrame({
        'hora': real_data['fecha'],
        'up': pred_data['Upper_Interval'],
        'low': pred_data['Lower_Interval']
    }).reset_index(drop=True)

    df_comparativo = pd.concat([df_real, df_pred], ignore_index=True)

    lineas = alt.Chart(df_comparativo).mark_line().encode(
        alt.X('hora:T').title('Hora'),
        alt.Y('bikes:Q').title('Demanda total'),
        alt.Color('type:N')
    ).properties(
        title = title
    )

    area = alt.Chart(df_interval).mark_area(opacity=0.2, color='#606ff1').encode(
        alt.X('hora:T').title('Hora'),
        alt.Y('up:Q').title('Intervalo superior'),
        alt.Y2('low:Q')
    )

    chart = alt.layer(area, lineas).resolve_scale(
        y='independent'
    )
    return chart

def crear_grafico_comparativo_returned(real_data, pred_data):
    df_real = pd.DataFrame({
        'fecha': real_data['fecha'],
        'total_bikes_returned': real_data['total_bikes_returned'],
        'type': 'Real'
    })

    df_pred = pd.DataFrame({
        'fecha': real_data['fecha'],
        'total_bikes_returned': pred_data,
        'type': 'Predicción'
    })

    df_comparativo = pd.concat([df_real, df_pred])

    chart = alt.Chart(df_comparativo).mark_line().encode(
        x='fecha:T',
        y='total_bikes_returned:Q',
        color='type:N'
    ).properties(
        title="Total bicicletas devueltas"
    )

    return chart


def get_popup_demanda(row, df):
    # leer los datos reales
    df_reales = get_last_24hours(df, row['station_id'])

    # leer las predicciones de la estación
    filename_taken = f"data/predicts/taken/{row['station_id']}_prediccion_taken.csv"
    filename_returned = f"data/predicts/returned/{row['station_id']}_prediccion_returned.csv"

    chart_concat_html = "<p>Predicción de la demanda no disponible</p>"
    if os.path.exists(filename_taken) & os.path.exists(filename_returned):
        df_pred_taken = read_csv_file(filename_taken)
        df_pred_returned = read_csv_file(filename_returned)

        chart_taken = crear_grafico_comparativo(df_reales, df_pred_taken, 'taken')               
        chart_returned = crear_grafico_comparativo(df_reales, df_pred_returned, 'returned')

        chart_concat = alt.hconcat(chart_taken, chart_returned)
        chart_concat_html = chart_concat.to_html()

    popup_text = f"""
                <span style='font-family:Arial;'>
                <h3>Estación: {row['station_id']} - {row['name']}</h3>
                <h4>Predicción de la demanda vs Realidad a 30/09/2024</h4>
                {chart_concat_html}
                </span>
                """

    iframe = folium.IFrame(popup_text, width=750, height=500)
    popup_form = folium.Popup(iframe)  
    return popup_form

def get_filtered_data(df):
    # Crea un control deslizante de tiempo
    min_fecha = df['fecha'].min()
    max_fecha = df['fecha'].max()
    start_date = datetime(min_fecha.year, min_fecha.month, min_fecha.day, 0)
    end_date = datetime(max_fecha.year, max_fecha.month, max_fecha.day, 23)

    fecha_inicio, fecha_fin  = st.slider(
        "Selecciona un rango de fechas",
        min_value=start_date,
        max_value=end_date,
        value=(start_date, end_date),
        step=timedelta(hours=1)
    )

    fecha_inicio_str = fecha_inicio.strftime('%d %b %Y, %I:%M%p')
    fecha_fin_str = fecha_fin.strftime('%d %b %Y, %I:%M%p')

    st.info('Inicio: **%s** Fin: **%s**' % (fecha_inicio_str,fecha_fin_str))

    df_filtrado = df[(df['fecha'] >= fecha_inicio) & (df['fecha'] <= fecha_fin)]

    return df_filtrado


def main():
    st.set_page_config(
        page_title="BikesGoSmart Dashboard",
        layout = "wide",
        initial_sidebar_state="auto"
    )
    
    with st.spinner("Creando la visualización. Por favor, espere..."):
        # Cargar datos de estaciones
        info_estaciones = read_csv_file("data/info_estaciones.csv")

        # Cargar datos de estado de bicing
        #estado_bicing_2021 = read_csv_file("data/estado_bicing_limpio_2021.csv")
        #estado_bicing_2022 = read_csv_file("data/estado_bicing_limpio_2022.csv")
        estado_bicing_2023 = read_csv_file("data/estado_bicing_limpio_2023.csv")
        estado_bicing_2024 = read_csv_file("data/estado_bicing_limpio_2024.csv")

        # Unir los dataframes en uno solo
        #estado_bicing = join_datframes(estado_bicing_2021, estado_bicing_2022, estado_bicing_2023, estado_bicing_2024)
        estado_bicing = pd.concat([estado_bicing_2023, estado_bicing_2024], ignore_index=True)

        #calcular la demanda total del dataset
        estado_bicing['rotacion'] = estado_bicing['total_bikes_taken'] + estado_bicing['total_bikes_returned']
        estado_bicing['fecha'] = pd.to_datetime(estado_bicing['fecha']).dt.tz_localize(None)
        
        df_filtrado = get_filtered_data(estado_bicing)

    with st.sidebar:
        selected = option_menu(
            menu_title="Estudio de la demanda",
            options=["Demanda total", "Eficiencia según demanda"],
        )
    
    if selected == "Demanda total":
        demanda_total = df_filtrado.groupby('station_id')['rotacion'].mean().reset_index()
        estaciones_demanda = pd.merge(demanda_total, info_estaciones, on='station_id', how='left')

        map_dem = get_mapa_bcn()
        marker_cluster = plugins.MarkerCluster().add_to(map_dem)
        for _, row in estaciones_demanda.iterrows():
            icon_pointer = folium.Icon(
                color='blue', 
                prefix='fa',
                icon='bicycle'
            )

            folium.Marker(
                location=(row['lat'], row['lon']),
                icon=icon_pointer,
                tooltip=get_tooltip_demanda(row),
                popup=get_popup_demanda(row, estado_bicing),
                lazy=True
            ).add_to(marker_cluster)

        st.header("Clasificación estaciones según demanda total", divider=True)
        st.components.v1.html(folium.Figure().add_child(map_dem).render(), width=775,height=750)
        #folium_static(map_dem,width=900, height=800)

    elif selected == "Eficiencia según demanda":
        # Tasa de eficiencia promedio agrupada por estación y unida a los datos de la estación
        tasa_promedio = df_filtrado.groupby('station_id')['tasa_eficiencia_ajustada'].mean().reset_index()
        estaciones_con_tasa = pd.merge(tasa_promedio, info_estaciones, on='station_id', how='left')

        map_ef = get_mapa_bcn()
        # Añadir marcadores para cada estación
        for _, row in estaciones_con_tasa.iterrows():
            icon_pointer = folium.Icon(
                color=obtener_color_tasa_general(row['tasa_eficiencia_ajustada']), 
                prefix='fa',
                icon='bicycle'
            )

            folium.Marker(
                location=(row['lat'], row['lon']),
                icon=icon_pointer,
                tooltip=get_tooltip_eficiencia(row)
            ).add_to(map_ef)

        st.header("Clasificación de las estaciones según su eficiencia", divider=True)
        st.components.v1.html(folium.Figure().add_child(map_ef).render(), width=775,height=750)
        #st.title("Eficiencia según demanda")

    # Mostrar el mapa en Streamlit
    #folium_static(m,width=1500, height=800)

if __name__ == "__main__":
    main()