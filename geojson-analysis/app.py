import streamlit as st
import folium
from pymongo import MongoClient
import pandas as pd
from shapely.geometry import shape, mapping
import geopandas as gpd
from streamlit_folium import folium_static

# Connect to MongoDB and fetch the documents
client = MongoClient("mongodb://mongodb:27017")
db = client['bicing_db']
collection = db['popular_stations']
geojson_docs = list(collection.find())

# Create a DataFrame from the flat structure
df = pd.DataFrame(geojson_docs)

# Convert the 'geometry' column to Shapely geometries
df['geometry'] = df['location'].apply(lambda x: shape(x) if isinstance(x, dict) else None)

# Create a GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry='geometry')

# Drop rows with invalid geometries
gdf = gdf[gdf['geometry'].notnull()]

# Set up Streamlit app
st.title("Barcelona Map with Coordinates")

# Create a Folium map centered around Barcelona
m = folium.Map(location=[41.3851, 2.1734], zoom_start=13)

# Add coordinates as markers
for idx, row in gdf.iterrows():
    point = mapping(row['geometry'])
    folium.Marker(
        location=[point['coordinates'][1], point['coordinates'][0]],
        popup=row['name']  # Adjust based on your properties
    ).add_to(m)

# Display the map in Streamlit
folium_static(m)
