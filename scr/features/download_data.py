from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pyspark
import pandas as pd
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

from pyspark_dist_explore import hist
import matplotlib.pyplot as plt

spark = SparkSession.builder \
  .appName("Test")  \
  .config("spark.yarn.access.hadoopFileSystems","abfs://data@datalakesii.dfs.core.windows.net/") \
  .config("spark.executor.memory", "24g") \
  .config("spark.driver.memory", "12g")\
  .config("spark.executor.cores", "12") \
  .config("spark.executor.instances", "24") \
  .config("spark.driver.maxResultSize", "12g") \
  .getOrCreate()

warnings.filterwarnings('ignore', category=DeprecationWarning)
sc=spark.sparkContext
sc.setLogLevel ('ERROR')

spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/propuesta_f29_comunidades/data_fuerza").createOrReplaceTempView("fuerza")

fuerza=spark.sql("select * from fuerza").toPandas()
fuerza.to_csv('data/external/fuerza_IVA_representante.csv', index=False)

spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/propuesta_f29_comunidades/data_contaminados_with_labels").createOrReplaceTempView("contaminados")

contaminados=spark.sql("select * from contaminados ").toPandas()
contaminados.to_csv('data/external/contaminados_processed_iva_representante_total_with_labels.csv', index=False)