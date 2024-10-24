import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, IntegerType
from analisis_functions import categorize_hour, get_season, read_dataset, save_dataset

import warnings
warnings.filterwarnings('always')
warnings.filterwarnings('ignore')

def complete_hour_days(spark, df, start_date, end_date):
    # Como la cantidad de datos que estamos tratando comprenden un período de tiempo muy grande, puede ser que en los datos históricos, en algún momento 
    # del período, se hayan creado estaciones nuevas. Por lo que solo rellenaremos con fechas y horas faltantes para aquellas estaciones que estuvieran
    # operativas a partir de su "fecha de creación" (obtenida a partir del primer día que aparecen en el dataset)

    # Creación del dataframe con todas las estaciones y su fecha de creación y retirada
    stations_df = df.groupBy("station_id").agg(
        F.min(F.to_date(F.concat_ws('-', "year", "month", "day"))).alias("creation_date"),
        F.max(F.to_date(F.concat_ws('-', "year", "month", "day"))).alias("end_date")
    )

    hours_df = spark.range(0, 24).toDF("hora_dia")
    
    fechas = pd.date_range(start=start_date, end=end_date, freq='D')
    fechas_pd = pd.DataFrame({
        'year': fechas.year,
        'month': fechas.month,
        'day': fechas.day
    })
    fechas_df = spark.createDataFrame(fechas_pd)
    
    # Combinación del dataframe de estaciones con todas las fechas y las horas. Y filtrado de aquellas combinaciones cuyas estaciones tengan un 
    # día, mes y año posterior a la fecha de creación 
    combinaciones = stations_df.crossJoin(fechas_df).crossJoin(hours_df)
    combinaciones_df = combinaciones.filter(
        (F.to_date(F.concat_ws('-', "year", "month", "day")) >= F.col("creation_date")) & 
        (F.to_date(F.concat_ws('-', "year", "month", "day")) <= F.col("end_date"))
    )

    # left join del dataframe original con el dataframe con las estaciones, fechas y horas combinadas. Aquellas que no estaban inicialmente tendrán los datos a NULL
    df_completo = combinaciones_df.join(df, on=["station_id", "year", "month", "day", "hora_dia"], how="left")
    df_completo = df_completo.dropDuplicates()
    
    return df_completo

def fill_interpolacion_lineal(df, window_spec):
    # Imputación de las columnas: num_bikes_available|num_docks_available|total_bikes_taken|total_bikes_returned|temperatura|precipitacion|humedad|viento
    # por el método de interpolación lineal "aproximada"
    # Como el dataset es muy grande y spark no tiene un método directo para realizar la interpolación, como tiene Pandas, lo hacemos a partir de la definición
    # de una ventana particionada por Id de estación y ordenada por fecha y hora. 
    # Cuando alguna de las columnas tiene un valor null se clacula la media de los valores adyacentes 

    # Separación de las columnas a interpolar según el tipo de datos para mantener el máximo de decimales cuando sea posible
    int_cols = ['num_bikes_available','num_docks_available','total_bikes_taken','total_bikes_returned','humedad']
    dec_cols = ['temperatura','precipitacion','viento']

    for col in int_cols:
        df = df.withColumn(
            col, 
            F.when(F.col(col).isNull(), 
                   F.round(F.mean(F.col(col)).over(window_spec)).cast(IntegerType())).otherwise(F.col(col))
        )

    for col in dec_cols:
        df = df.withColumn(
            col, 
            F.when(F.col(col).isNull(), 
                   F.mean(F.col(col)).over(window_spec)).otherwise(F.col(col))
        )

    return df

def station_tratement(df):
    # descartamos las estaciones donde end_date<'2024-07-31' & numdiasServicio<365    
    df = df.withColumn("numDiasServicio", F.datediff(F.col('end_date'), F.col('creation_date')))
    df_filtered = df.filter((F.col("end_date") >= '2024-07-31') | 
                            (F.col("numDiasServicio") > 365)
                            )
    
    # Añadimos la información de la estación al dataset (nombe, dirección, etc.)
    #df_stations = read_dataset(spark, "/opt/data/info_estaciones.csv")
    #df_merged = df_filtered.join(df_stations.select("station_id", "name", "post_code"), on="station_id", how="left")
    df_filtered = df_filtered.drop("numDiasServicio")

    return df_filtered

def fill_null_values(df):
    #Imputación de la columna last_date_reported a partir de los valores de las columnas: day, month, year y hora_dia
    date_col = F.concat_ws("-", 
                           F.col("year").cast(StringType()),
                           F.lpad(F.col("month").cast(StringType()), 2, '0'),
                           F.lpad(F.col("day").cast(StringType()), 2, '0')
                           )
    date_time_col = F.to_timestamp(F.concat(date_col, F.lit(" "), F.lpad(F.col("hora_dia").cast(StringType()), 2, '0'), F.lit(":00:00")), "yyyy-MM-dd HH:mm:ss")
    df = df.withColumn("last_reported_local", 
                       F.when(F.col("last_reported_local").isNull(), date_time_col).otherwise(F.col("last_reported_local")))

    #Imputación columnas hora_info, season, weekday, num_bikes_error = 0
    categorize_hour_udf = F.udf(categorize_hour, StringType())
    get_season_udf = F.udf(get_season, StringType())
    df = df.withColumn("hora_info", 
                       F.when(F.col("hora_info").isNull(), categorize_hour_udf(F.col("hora_dia"))).otherwise(F.col("hora_info"))) \
            .withColumn("season",
                        F.when(F.col("season").isNull(), get_season_udf(F.to_date(F.col("last_reported_local")))).otherwise(F.col("season"))) \
            .withColumn("weekday", 
                        F.when(F.col("weekday").isNull(), ((F.dayofweek('last_reported_local')+5)%7)+1).otherwise(F.col("weekday"))) \
            .fillna({'num_bikes_error': 0})
    
    # Imputación de las columnas status_station y festivo mediante la técnica del fordward fill
    window_spec = Window.partitionBy("station_id").orderBy("last_reported_local").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = df.withColumn("status_station", F.last("status_station", True).over(window_spec)) \
            .withColumn("festivo", F.last("festivo", True).over(window_spec))
    
    # Imputación de los valores faltantes para la columna "laborable" a partir del valor en la columna 
    # festivo y weekday para aquellas celdas que son Null
    df = df.withColumn("laborable",
                    F.when(F.col("laborable").isNull(), 
                           F.when((F.col("festivo") == 0) & (F.col("weekday").between(1, 5)), F.lit(1)).otherwise(F.lit(0))
                           ).otherwise(F.col("laborable"))
                    )
    
    df.persist()
    
    df = fill_interpolacion_lineal(df, window_spec)

    df.unpersist()

    return df

# def explore_data(df):    
#     df_stations = df.select("station_id", "name").distinct()
#     num_estaciones = df_stations.count()
#     print(f"Número total de estaciones: {num_estaciones}")

    
def main():
    spark = SparkSession.builder \
        .appName("Preprocessing Data") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.cores", "6") \
        .getOrCreate()
    
    spark.sparkContext.addPyFile("explotation_zone/analisis_functions.py")

    #Lectura del dataset generado con los datos del estado de las estaciones del bicing + datos complementarios
    df = read_dataset(spark, "/opt/data/estado_bicing.csv")
    df.persist()

    # Cumplimentación del dataset con los días y horas faltantes
    df_completo = complete_hour_days(spark, df, '2021-01-01', '2024-09-30')
    df_completo.persist()
    num_df_completo=df_completo.count()
    print(f"Núm líneas df completo: {num_df_completo}")
    
    # Descartamos estaciones no validas y completamos el dataset con la información de las 
    # estaciones (nombre, dirección, etc.)
    df_con_estaciones = station_tratement(df_completo)
    df_con_estaciones.persist()
    
    # Completamos el dataset con los valores faltantes
    df_filled = fill_null_values(df_con_estaciones)
    df_filled.persist()

    # explore_data(df_filled)
    num_df_filled=df_filled.count()
    print(f"Núm líneas df completo: {num_df_filled}")

    # Guardado del dataset completo para el posterior análisis de los datos
    save_dataset(df_filled, "/opt/data/estado_bicing_completo.csv")
    
    df.unpersist()
    del df
    df_completo.unpersist()
    del df_completo
    df_con_estaciones.unpersist()
    del df_con_estaciones
    df_filled.unpersist()
    del df_filled
        
    spark.stop()
    
if __name__ == "__main__":
    main()
