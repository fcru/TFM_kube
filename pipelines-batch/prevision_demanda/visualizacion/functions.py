import pandas as pd
import numpy as np
import streamlit as st

# Función para determinar el color según la tasa de eficiencia
def obtener_color_tasa_general(tasa):
    if tasa < 0.4:
        return 'red'
    elif tasa < 0.5:
        return 'orange'
    elif tasa < 0.6:
        return 'blue'
    else:
        return 'green'
    
def capitalizar_nombre(nombre):
    palabras_excepcion = ['de', 'del', 'la', 'las', 'el', 'los', 'en', 'y', 'o']
    texto_capitalizado = nombre.title()
    palabras = texto_capitalizado.split()
    resultado = [palabra if palabra.lower() not in palabras_excepcion else palabra.lower() for palabra in palabras]
    
    return ' '.join(resultado)

@st.cache_data
def join_datframes(df1, df2, df3, df4):
    df = pd.concat([df1, df2, df3, df4], ignore_index=True)
    return df

@st.cache_data
def read_csv_file(filename):
    df = pd.read_csv(filename)

    return df

def get_last_24hours(df, station_id):
    df_station = df[df['station_id'] == station_id]
    df_station = df_station.sort_values(by = 'fecha')
    df_last_24h = df_station.tail(24).reset_index(drop=True)

    return df_last_24h






###################################################################
## Funciones para el preprocesado de los datos
###################################################################
def save_estado_bicing(df, filename):
    df.to_csv(filename, index=False)

def format_data_estado_bicing(df):
    df['num_bikes_available'] = df['num_bikes_available'].astype(int)
    df['num_docks_available'] = df['num_docks_available'].astype(int)
    df['num_bikes_error'] = df['num_bikes_error'].astype(int)

    return df

def forwardfill_availability(df):
    condicion = (df['num_bikes_available'] == 0) & (df['num_docks_available'] == 0)
    df.loc[condicion, ['num_bikes_available', 'num_docks_available']] = None
    # Aplicar forward fill (ffill) en todo el dataframe pero solo en esas dos columnas
    df[['num_bikes_available', 'num_docks_available']] = df[['num_bikes_available', 'num_docks_available']].ffill()

    return df

def calculate_tasa_eficiencia_ajustada(df):
    df['tasa_eficiencia_ajustada'] = (
    (0.5 + 0.5 * (df['num_bikes_available'] / 
                  (df['num_bikes_available'] + df['num_docks_available']))) *
    (1 - (np.abs(df['total_bikes_taken'] - df['total_bikes_returned']) / 
          (df['total_bikes_taken'] + df['total_bikes_returned'] + 1)))
    ) * np.where(df['num_bikes_available'] == 0, 0.1, 1) * np.where(df['num_docks_available'] == 0, 0.1, 1)

    return df