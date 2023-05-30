from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, Tokenizer, IDF
import numpy as np
from pyspark.sql.functions import udf
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.sql.functions import size

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, Tokenizer, HashingTF, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
if __name__ == "__main__":
  spark = SparkSession\
  .builder\
  .appName("pipeline_news_corte2")\
  .getOrCreate()
  # Leer los datos desde el archivo CSV
  data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("contenido_2023-04-28 (1).csv")

  # Obtener las columnas de características y la etiqueta de predicción
  features_col = [data.columns[i] for i in range(1, len(data.columns)) if i != 2]
  prediction_label = list(set(data.columns) - set(features_col))

  indexer = StringIndexer(inputCol="Section", outputCol="label")
  data = indexer.fit(data).transform(data)

  # Aplicar el StringIndexer a las columnas de características
  indexer = StringIndexer(inputCols=features_col, outputCols=["{}_index".format(col) for col in features_col])
  data = indexer.fit(data).transform(data)

  # Utilizar VectorAssembler para combinar las columnas indexadas en una columna "Features"
  assembler = VectorAssembler(inputCols=["{}_index".format(col) for col in features_col], outputCol="Features")
  data = assembler.transform(data)

  # Definir las etapas del pipeline
  tokenizer = Tokenizer(inputCol="Titular", outputCol="words")
  hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
  idf = IDF(inputCol="rawFeatures", outputCol="features")
  lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=30, threshold=0.5, regParam=0.1)
  # Crear el pipeline
  pipeline = Pipeline(stages=[tokenizer, hashingTF, idf,lr])

  # Ajustar el pipeline a los datos
  model = pipeline.fit(data)
  # Realizar predicciones en el conjunto de prueba
  predictions = model.transform(data)
  predictions.show()

  evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

  # Calcular la métrica de evaluación (ejemplo: área bajo la curva ROC)
  auc = evaluator.evaluate(predictions)
  #predictions.write.format("txt").save("edwins.txt")
  # Imprimir la métrica de evaluación
  print("Área bajo la curva ROC:", auc)
