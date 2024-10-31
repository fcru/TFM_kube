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

def get_tooltip(row, type):
    if type == 'demanda':
        tasa = f"<b>Demanda total</b>: {row['rotacion']:.2f}"
    elif type == 'eficiencia':
        tasa = f"<b>Eficiencia</b>: {row['tasa_eficiencia_ajustada']:.2f}"

    text = "<span style='font-family:Arial;'> " +\
            f"<b>ID estación</b>: {row['station_id']}<br>" +\
            f"<b>Nombre</b>: {capitalizar_nombre(row['name'])}<br>" +\
            f"{tasa} </span>"
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
                <h4>Nivel de rotación: {row['rotacion']:.2f}</h4>
                <h4>Predicción de la demanda vs Realidad a 30/09/2024</h4>
                {chart_concat_html}
                </span>
                """

    iframe = folium.IFrame(popup_text, width=900, height=550)
    popup_form = folium.Popup(iframe)  
    return popup_form


def config_filters(df):
    min_fecha = df['fecha'].min()
    max_fecha = df['fecha'].max()

    start_date = datetime(min_fecha.year, min_fecha.month, min_fecha.day)
    end_date = datetime(max_fecha.year, max_fecha.month, max_fecha.day)

    fecha_inicio, fecha_fin = st.sidebar.slider(
        "Seleccione un rango de fechas",
        min_value=start_date,
        max_value=end_date,
        value=(start_date, end_date),
        step=timedelta(days=1)
    )

    hour_ini, hour_fin = st.sidebar.select_slider(
        "Seleccione un rango de horas",
        options=['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                 '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00',
                 '18:00','19:00','20:00','21:00','22:00','23:00'],
        value=('00:00','23:00')
    )

    hora_inicio = datetime.strptime(hour_ini, '%H:%M').time()
    hora_fin = datetime.strptime(hour_fin, '%H:%M').time()
    fecha_inicio = datetime.combine(fecha_inicio.date(), hora_inicio)
    fecha_fin = datetime.combine(fecha_fin.date(), hora_fin)

    fecha_inicio_str = fecha_inicio.strftime('%d %b %Y, %H:%M') + ' h'
    fecha_fin_str = fecha_fin.strftime('%d %b %Y, %H:%M') + ' h'

    st.sidebar.info('Inicio: **%s**  \nFin: **%s**' % (fecha_inicio_str,fecha_fin_str))

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
        
        df_filtrado = config_filters(estado_bicing)
        #df_filtrado = get_filtered_data(estado_bicing)

    tab_demanda, tab_eficiencia = st.tabs(["Predicción demanda", "Eficiencia estaciones"])

    with tab_demanda:
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
                tooltip=get_tooltip(row, 'demanda'),
                popup=get_popup_demanda(row, estado_bicing),
                lazy=True
            ).add_to(marker_cluster)

        st.subheader("Demanda de las estaciones", divider=True)
        figure = folium.Figure().add_child(map_dem)
        st.components.v1.html(figure.render(), width=1200,height=750)

    with tab_eficiencia:
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
                tooltip=get_tooltip(row, 'eficiencia')
            ).add_to(map_ef)

        st.subheader("Clasificación de las estaciones según su eficiencia", divider=True)
        figure = folium.Figure().add_child(map_ef)
        st.components.v1.html(figure.render(), width=1200,height=750)
        
    
if __name__ == "__main__":
    main()