import json
from bson import ObjectId
from geojson import Point
from pymongo import MongoClient
import pandas as pd
from shapely import wkt
from shapely.geometry import shape, mapping
import geopandas as gpd
from bson.son import SON

def safe_shape(geom):
    if isinstance(geom, dict):
        return shape(geom)
    elif isinstance(geom, str):
        try:
            return wkt.loads(geom)
        except Exception as e:
            print(f"Error parsing WKT: {str(e)}")
            return None
    elif isinstance(geom, list) and len(geom) == 2:
        return Point(geom)
    else:
        return None

def clean_document(doc):
    """
    Convert ObjectId and other non-serializable types to serializable ones.
    """
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
        # Add any other necessary conversions here (e.g., for datetime)
    return doc

def filter_estacio_to_gdf(df_to_filter):
    client = MongoClient("mongodb://mongodb:27017")
    db = client['bicing_db']
    collection = db['estacio']
    try:
        station_ids_to_exclude = df_to_filter['station_id'].tolist()

        pipeline = [
            # Match stage to filter out the station_ids present in the other DataFrame
            {"$match": {
                "station_id": {"$nin": station_ids_to_exclude}
            }},
            # Group by station_id and get the first document for each group
            {"$group": {
                "_id": "$station_id",
                "first_doc": {"$first": "$$ROOT"}
            }},
            # Replace the root with the first document
            {"$replaceRoot": {"newRoot": "$first_doc"}}
        ]

        # Execute the aggregation
        data = list(collection.aggregate(pipeline))

        df = pd.DataFrame(data)

        # Convert geometry
        if 'geometry' in df.columns:
            df['geometry'] = df['geometry'].apply(safe_shape)
        elif 'coordinates' in df.columns:
            df['geometry'] = df['coordinates'].apply(
                lambda x: Point(x) if isinstance(x, list) and len(x) == 2 else None)
        else:
            return None, f"No geometry or coordinates found in collection estacio"

        gdf = gpd.GeoDataFrame(df, geometry='geometry').dropna(subset=['geometry'])
        if len(gdf) == 0:
            return None, f"No valid geometries found in collection estacio"

        gdf.set_crs(epsg=4326, inplace=True)
        return gdf, None
    except Exception as e:
        return None, f"Error processing estacio: {str(e)}"

def doc_to_gdf(collection_name, limit=None, fields=None):
    try:
        client = MongoClient("mongodb://mongodb:27017")
        db = client['bicing_db']
        collection = db[collection_name]

        # Use projection to limit fields if specified
        projection = {field: 1 for field in fields} if fields else None

        # Use limit to reduce the number of documents
        if limit:
            data = list(collection.find({}, projection).limit(limit))
        else:
            data = list(collection.find({}, projection))

        if not data:
            return None, f"No data found in collection {collection_name}"
        data = [clean_document(doc) for doc in data]

        df = pd.DataFrame(data)

        # Convert geometry
        if 'geometry' in df.columns:
            df['geometry'] = df['geometry'].apply(safe_shape)
        elif 'coordinates' in df.columns:
            df['geometry'] = df['coordinates'].apply(
                lambda x: Point(x) if isinstance(x, list) and len(x) == 2 else None)
        else:
            return None, f"No geometry or coordinates found in collection {collection_name}"

        gdf = gpd.GeoDataFrame(df, geometry='geometry').dropna(subset=['geometry'])
        if len(gdf) == 0:
            return None, f"No valid geometries found in collection {collection_name}"

        gdf.set_crs(epsg=4326, inplace=True)
        return gdf, None
    except Exception as e:
        return None, f"Error processing {collection_name}: {str(e)}"


