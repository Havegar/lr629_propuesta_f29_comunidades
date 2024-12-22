

### Algoritmo de Louvain y su aplicacion a la red de fuerzas para la deteccion de comunidades mediante maximizaicon de la modularidad con diferentes resoluciones.


from pyspark.sql import SparkSession


import pandas as pd
import networkx as nx
from cdlib import algorithms
import os
from networkx.algorithms.community import modularity
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

spark = SparkSession.builder \
      .appName("Ejecucion algoritmo de louvain")  \
      .config("spark.yarn.access.hadoopFileSystems","abfs://data@datalakesii.dfs.core.windows.net/") \
      .config("spark.executor.memory", "15g") \
      .config("spark.driver.memory", "12g")\
      .config("spark.executor.cores", "2") \
      .config("spark.executor.instances", "5") \
      .config("spark.driver.maxResultSize", "12g") \
      .getOrCreate()

warnings.filterwarnings('ignore', category=DeprecationWarning)
sc=spark.sparkContext
sc.setLogLevel ('ERROR')


# Cargar datos desde el archivo de la combinacion de ambas fuerzas
spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/propuesta_f29_comunidades/data_fuerza").createOrReplaceTempView("fuerza")

df_relaciones=spark.sql("select * from fuerza").toPandas()

spark.read.parquet("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/propuesta_f29_comunidades/data_contaminados_with_labels").createOrReplaceTempView("contaminados")
df_existente=spark.sql('select * from contaminados').toPandas()

# Crear un DataFrame final para almacenar los resultados
df_final = df_existente.copy()

# Detener uso de Spark
spark.stop()


#Tambien es posible leer los datos desde el espacio de trabajp en un csv, si fuese el caso
#df_relaciones = pd.read_csv("data/external/fuerza_IVA_representante.csv")

# Crear un grafo dirigido ponderado, donde el ponderador sera la fuerza de la relacion
G = nx.DiGraph()
for _, row in df_relaciones.iterrows():
    G.add_node(row['emisor'])
    G.add_node(row['receptor'])
    G.add_edge(row['emisor'], row['receptor'], fuerza=row['Fi'])

# Convertir el grafo dirigido a un grafo no dirigido ponderado por la fuerza de la relacion
H = nx.Graph(G)
for u, v, data in G.edges(data=True):
    if H.has_edge(u, v):
        H[u][v]['fuerza'] += data['fuerza']
    else:
        H.add_edge(u, v, fuerza=data['fuerza'])
# Aplicar el algoritmo de Louvain con peso correspondiente, maximizando la modularidad de la red.
# Un valor de resolución cercano a 0 tiende a producir comunidades más grandes y menos densas. En este caso, el algoritmo # prioriza la detección de comunidades más amplias con conexiones más dispersas.

# Un valor de resolución cercano a 1 tiende a producir comunidades más pequeñas y más densas. En este caso, el algoritmo # prioriza la detección de comunidades más específicas con conexiones más densas entre sus nodos.

# Definimos los valores del hiperparámetro de resolución que se quiere explorar
valores_resolucion = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]

# Leer el archivo 'contaminados_processed_iva_representante_total_with_labels.csv'
#existing_dataframe_path = 'data/external/contaminados_processed_iva_representante_total_with_labels.csv'
#df_existente = pd.read_csv(existing_dataframe_path)


# Iterar sobre los diferentes valores de resolución
for resolucion in valores_resolucion:

    # Aplicar el algoritmo de Louvain con el valor de resolución actual
    louvain_result = algorithms.louvain(H, weight='fuerza', resolution=resolucion)

    # Calcular la modularidad utilizando la función modularity de networkx
    modularity_value = modularity(H, louvain_result.communities, weight='fuerza')

    # Crear un DataFrame para mostrar nodos, comunidades y modularidad
    df_comunidades = pd.DataFrame(columns=['cont_rut', f'comunidad_{resolucion}', f'modularidad_{resolucion}'])

    df_comunidades_list = []
    # Llenar el DataFrame con las comunidades detectadas
    for idx, comunidad in enumerate(louvain_result.communities):
        for node in comunidad:
            df_comunidades_list.append({'cont_rut': node, f'comunidad_{resolucion}': idx+1, f'modularidad_{resolucion}': modularity_value})
    df_comunidades = pd.concat([df_comunidades, pd.DataFrame(df_comunidades_list)], ignore_index=True)

    # Fusionar DataFrames utilizando la columna 'cont_rut'
    df_final = pd.merge(df_final, df_comunidades, on='cont_rut', how='left')

# Eliminar columnas redundantes
df_final = df_final.drop_duplicates()

#df_to_parquet=spark.createDataFrame(df_final)
# Guardar el archivo final parquet en la carpeta del datalake del proyecto, para poder utilizarlo en su analisis pronto.
#df_to_parquet.write.mode('overwrite').format("parquet").save("abfs://data@datalakesii.dfs.core.windows.net/DatosOrigen/lr-629/propuesta_f29_comunidades/comunidades_louvain_extended")

# Guardar el DataFrame final en un archivo CSV, en caso requerido
output_directory = '/home/cdsw/data/processed/comunidades_louvain/'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
df_final.to_csv(os.path.join(output_directory, 'comunidades_resolution_study_louvain.csv'), index=False)
    
