from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType
from datetime import datetime
import pandas as pd

def read_dataset(spark):
    df = spark.read.csv("C:/users/U156294/PycharmProjects/TFM_kube/data/estado_bicing.csv", header=True,
                        inferSchema=True, sep=",")
    return df

def complet_hour_days(spark, df, start_date, end_date):
    station_df = df.select("station_id").distinct()
    
    fechas = pd.date_range(start=start_date, end=end_date, freq='h')
    
    schema = StructType([StructField("last_reported_local", TimestampType(), True)])
    fechas_tuples = [(datetime.strptime(str(fecha), "%Y-%m-%d %H:%M:%S"),) for fecha in fechas]
    
    fechas_df = spark.createDataFrame(fechas_tuples, schema)
    fechas_df.printSchema()
    
    fechas_df = fechas_df.withColumn("year", F.year("last_reported_local")) \
                         .withColumn("month", F.month("last_reported_local")) \
                         .withColumn("day", F.dayofmonth("last_reported_local")) \
                         .withColumn("hora_dia", F.hour("last_reported_local"))
    
    combinaciones_df = station_df.crossJoin(fechas_df)
    df_completo = combinaciones_df.join(df, on=["station_id", "hora_dia", "year", "month", "day"], how="left")
    
    return df_completo
    
def main():
    spark = SparkSession.builder \
        .appName("Análisis demanda") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.cores", "6") \
        .getOrCreate()
    
    df = read_dataset(spark)
    df.persist()
    df_completo = complet_hour_days(spark, df, '2021-01-01', '2024-07-31')
    df_completo.persist()
    df_filtrado = df_completo.filter((F.col("year")==2023) & (F.col("month")==4) & (F.col("day")==1))
    df_filtrado.persist()
    df_filtrado.coalesce(1).write.mode("overwrite").csv("C:/users/U156294/PycharmProjects/TFM_kube/data/estado_bicing_completo.csv", header=True)
    print("Fichero guardado")
    
    df.unpersist()
    del df
    df_completo.unpersist()
    del df_completo
    df_filtrado.unpersist()
    del df_filtrado
    
    spark.stop()
    
if __name__ == "__main__":
    main()
