import sys
import time
from explotation_zone.analisis_functions import read_dataset
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler, StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression

import warnings
warnings.filterwarnings('always')
warnings.filterwarnings('ignore')

def get_train_test_data(df):
    train_df = df.filter((F.to_date(F.col("last_reported_local")) >= "2021-01-01") & 
                         (F.to_date(F.col("last_reported_local")) <= "2023-12-31"))
    num_train = train_df.count()
    print(f"Número registros TRAIN: {num_train}")
    test_df = df.filter((F.to_date(F.col("last_reported_local")) >= "2024-01-01") & 
                        (F.to_date(F.col("last_reported_local")) <= "2024-07-31"))
    num_test = test_df.count()
    print(f"Número registros TEST: {num_test}")

    return train_df, test_df

def regresion_lineal(train_df, test_df):
    # Selección de las variables predictoras y la variable objetivo (label)
    # Consideramos como variable objetivo la variable total_bikes_taken, aunque también podríamos haber considerado la variable total_bikes_returned
    train_df = train_df.select("station_id", "hora_dia", "laborable", "season", "temperatura", "precipitacion", "humedad", "viento", "total_bikes_taken")
    test_df = test_df.select("station_id", "hora_dia", "laborable", "season", "temperatura", "precipitacion", "humedad", "viento", "total_bikes_taken")

    # Transformación de variables categóricas medianta OneHotEncoding
    season_indexer = StringIndexer(inputCol="season", outputCol="season_index")
    station_indexer = StringIndexer(inputCol="station_id", outputCol="station_index")
    season_encoder = OneHotEncoder(inputCol="season_index", outputCol="season_vec")
    station_encoder = OneHotEncoder(inputCol="station_index", outputCol="station_vec")
    hora_encoder = OneHotEncoder(inputCol="hora_dia", outputCol="hora_vec")

    # Definición de las variables predictoras (features)
    input_cols = ["hora_vec", "season_vec", "station_vec", "laborable", "temperatura", "precipitacion", "humedad", "viento"]
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

    # Escalado de variables predictoras
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)

    # Definición del modelo de regresión lineal regularizada
    lr = LinearRegression(featuresCol="scaled_features", labelCol="total_bikes_taken", regParam=0.1)
    
    pipeline = Pipeline(stages=[season_indexer, station_indexer, season_encoder, 
                                station_encoder, hora_encoder, assembler, scaler, lr])

    # Medidor del rendimiento del modelo
    evaluator_rmse = RegressionEvaluator(labelCol='total_bikes_taken', predictionCol='prediction', metricName='rmse')

    # Grid de parámetros (parámetros de regularización, Ridge o Lasso y número de iteraciones)
    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1]) \
        .addGrid(lr.elasticNetParam, [0.0, 1.0]) \
        .addGrid(lr.maxIter, [10, 20]) \
            .build()
    
    #Validación cruzada
    crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator_rmse,
                          numFolds=3)

    # Entrenamiento del modelo
    start_time = time.time()
    cvModel = crossval.fit(train_df)
    end_time  = time.time()
    total_time_training = (((end_time - start_time)/60)/60)
    bestModel = cvModel.bestModel

    print(f"Tiempo total de entrenamiento: {total_time_training}")

    best_regParam = bestModel.stages[-1]._java_obj.parent().getRegParam()
    best_elasticNetParam = bestModel.stages[-1]._java_obj.parent().getElasticNetParam()
    best_maxIter = bestModel.stages[-1]._java_obj.parent().getMaxIter()
        
    predictions = bestModel.transform(test_df).select("total_bikes_taken", "prediction", "scaled_features")
    rmse = evaluator_rmse.evaluate(predictions)
    
    # Evaluar el modelo usando R2 (coeficiente de determinación)
    evaluator_r2 = RegressionEvaluator(labelCol="total_bikes_taken", predictionCol="prediction", metricName="r2")
    r2 = evaluator_r2.evaluate(predictions)
    
    print("Resultados finales modelo Regresión lineal:")
    print("==========================================")
    print(f"  - Mejor valor de regularización (regParam): {best_regParam}")
    print(f"  - Proporción de Lasso a Ridge (elasticNetParam): {best_elasticNetParam}")
    print(f"  - Número de iteraciones máximo (maxIter): {best_maxIter}")
    print("")
    print(f"  - Root Mean Squared Error (RMSE): {rmse}")
    print(f"  - R Squared (R2): {r2}")


def main(test):
    # Inicialización de la sesión de Spark
    spark = SparkSession.builder \
        .appName("Análisis demanda") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.cores", "6") \
        .getOrCreate()
    
    spark.sparkContext.addPyFile("explotation-zone/analisis_functions.py")
    spark.sparkContext.setLogLevel("ERROR")

    df = read_dataset(spark, "/opt/data/estado_bicing_completo.csv")
    df.persist()
    # df.printSchema()

    # División de los datos entre test y training
    train_df, test_df = get_train_test_data(df)
    train_df.persist()
    test_df.persist()

    if test == 1:
        regresion_lineal(train_df, test_df)  

    df.unpersist()
    del df
    train_df.unpersist()
    del train_df
    test_df.unpersist()
    del test_df

    spark.stop()

def show_options():
    print("Elije qué análisis quieres ejecutar:")
    print("\t 0 - Salir")
    print("\t 1 - Regresión lineal")

if __name__ == "__main__":
    show_options()
    op = int(input())

    while op != 0:
        if op >= 1:
            main(op)
        else:
            print ("Exiting ...")
            sys.exit()

        show_options()
        op = int(input())