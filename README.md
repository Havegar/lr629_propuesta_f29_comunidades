## Tabla de contenidos
1. [Informacion General](#Informacion_general)
2. [Objetivo](#Objetivo)
3. [Colaboradores](#Colaboradores)
4. [Instalacion](#Instalacion)
5. [Tecnologias](#Tecnologias)
6. [Recursos CML](#Recursos_CML)
7. [Ejecucion_busqueda de comunidades](#Ejecucion_busqueda_comunidades)
8. [Ejecucion_estadistica_grupos](#Ejecucion_estadistica_grupos)
9. [Ejecucion_IVA_grupo](#Ejecucion_IVA_grupos)
10. [Sugerencias y pasos siguientes](#Sugerencias)
11. [Estructura de carpetas](#plantilla_proyectos_python)

### Informacion general
***
En este proyecto se avanza en la caracterizacion de comunidades de personas juridicas obtenidas por diferentes metodos de agrupacion, como lo son el Algoritmo de Louvain en diferentes valores del parametro resolucion y tambien  la agrupacion de sociedades por persona natural con participacion con el objetivo de caracterizar la emision de IVA intra y extragrupo, junto a otros indices previos agregados en el proyecto LR629_Riesgo_Contaminación, tales como numero de documentos emitidos, tiempo de vida del contribuyente, declaracion de F29 y unidad geografica asociada. Junto con ello se hace una comparacion de estos metodos de agrupacion con grupos economicos reales. 

### Colaboradores
***
En este proyecto participa Henry Vega, Data Analyst de APIUX asi como el equipo del Área de Riesgo e Informalidad del SII, compuesto por Daniel Cantillana, Jorge Bravo y Yasser Nanjari, además Arnol Garcia, jefe de área y Manuel Barrientos, con soporte técnico en el espacio de trabajo y Jorge Estéfane, jefe de proyecto por parte de APIUX.

### Instalacion
***
Los pasos para instalar el proyecto y ejecutarlo son: 
1. Instalar paquetes con el notebook 'install_packages.ipynb'
2. Ejecutar notebooks de Python en el orden indicado (puede ser tambien el job asociado si esta disponible esta funcionalidad)

## Tecnologias
***
Una lista de las tecnologias mas importantes utilizadas,tanto para manejo de datasets, graficos, algoritmos de grafos, entre otros.

- pyspark.sql
- pyspark.sql.functions 
- pyspark 
- pandas 
- warnings
- networkx 
- cdlib 
- os
- networkx.algorithms.community
- matplotlib.pyplot
- seaborn 
- IPython.display
- pyspark_dist_explore
- matplotlib.pyplot
- pyspark.sql.types

## Recursos_CML
***
Todo la implementacion  de los porcesos de calculo se realizaron en CML en notebooks de pyhton como en ejecutables '.py' utilizando SQL en Spark para poder hacer la consulta de las tablas, filtrado y los procesos ETL correspondientes. Tambien se implementaron dos jobs:
* download_data: job para descargar la data de fuerzas que se utilizara para ejecutar el algoritmo de Louvain.
* comunidades_louvain_algorithm_expanded: job para ejecutar el algoritmo de Louvain con diferentes resoluciones.

Se utilizo Spark 3.2.3 y Python 3.10 para crear el codigo y las correspondientes consultas y procesamiento de tablas utilizando algoritmos iterativos  mediante SQL.

## Busqueda de comunidades de Louvain para diferentes resoluciones
***
Para poder hacer un estudio de como afecta el hiperparametro de resolucion en la construccion de comunidades se ejecutan los archivos en le orden siguiente:


1. `data/external/upload.py`:contiene el script que permite obtener la data de fuerza entre entidades desde un archivo en Azure y lo deja en la siguiente ruta: `data/external/fuerza_IVA_representante.csv`. Ademas de ello descarga los datos de los nodos(contribuyentes) en  `data/external/contaminados_processed_iva_representante_total_with_labels.csv`. El script esta regulado por el job **comunidades_louvain_algorithm_expanded** y tiene como output el archivo **/home/cdsw/data/processed/contaminados_resolution_study.csv**.

2. `scr/algorithms/comunidades_louvain_algorithm_expanded.py`: contiene el script tiene comp input los archivos importados del paso anterior y agrega las comunidades calculadas por resolucion a cada contribuyente en varias iteraciones del algoritmo de Louvain y lo guarda posteriormente en el archivo `data/processed/comunidades_louvain/comunidades_resolution_study_louvain.csv`.

3. En el  notebook `scr/notebooks/comunidades_louvain/louvain_resolucion_performance.ipynb` se realiza un analisis de la agrupacion de entidades en comunidades en funcion de la resolucion, lo que permite analizar la performance el algoritmo y justificar el uso de algun valor del hiperparametro.



## Busqueda de comunidades con porcentaje de participacion de una persona natural
***
Para el caso de las comunidades asociadas a una persona natural por su porcentaje de participacion, se deben ejecutar los archivos en el orden siguiente:

1. `scr/features/depuracion_sociedades.ipynb`: archivo que procesa la malla societaria y procesa los outliers y errores de los porcentajes de participacion, posteriormente guarda el output en `/home/cdsw/data/processed/sociedades_participacion_capital_nozero.csv` .

2. `scr/algorithms/comunidades_sociedades_por_contribuyente.ipynb`: notebook que realiza las operaciones iterativas sobre la malla societaria, obteniendo el archivo `/home/cdsw/data/processed/comunidades_persona_natural/comunidades_natural_sociedades.csv`  que contiene la sociedad asociada con un porcentaje de participacion de la persona natural en la misma y sus princiaples caracteristicas obtenidaes en el proyecto LR629_Riesgo_Contaminación.



## Ejecucion estadistica comunidades
***

Los notebooks que permiten obtener informacion agregada y estadisticas para cada grupo (definido tanto por Louvain como por persona natural participante) son respectivamente `scr/notebooks/comunidades_louvain/EDA_comunidades_louvain.ipynb` y `scr/notebooks/comunidades_persona_natural/EDA_comunidades_persona_natural.ipynb` Estos notebooks tiene como output archivos con informacion agregada, los cuales son respectivamente `/home/cdsw/data/processed/comunidades_louvain/estadistica_comunidades_louvain.csv` y `/home/cdsw/data/processed/comunidades_persona_natural/estadistica_comunidades_persona_natural.csv`

## Ejecucion IVA comunidades
***

La data anterior se complementa con mediciones del flujo de IVA para cada grupo, tanto al interior como al exterior del grupo. El notebook que realiza este proceso con las comunidades de Louvain son `scr/notebooks/comunidades_louvain/IVA_comunidades_louvain.ipynb` y con respecto a los grupos definidos por persona natural, `scr/notebooks/comunidades_persona_natural/IVA_comunidades_persona_natural.ipynb`. Los ouputs de cad anotebook son respectivamente `/home/cdsw/data/processed/estadistica_comunidades_louvain_with_iva_balance.csv` y `/home/cdsw/data/processed/comunidades_persona_natural/estadistica_comunidades_persona_natural_with_iva_balance.csv`.

## Comparativa con grupos economicos reales
***
Se realiza una comparativa de grupos economicos reales con las comunidades encontradas por diferentes metodos,en el notebook `scr/notebooks/evaluacion_grupos_economicos_louvain.ipynb`.

## Sugerencias y pasos siguientes
***
Dadas estas dos formas de creacion de comunidades, una basada en una expansion de algoritmo de Louvain, otra mediante agrupacion por persona natural asociada y la comparacion de grupos economicos conocidas, se propone estudiar estos grupos para  caracterizarlos. De esta forma la fuerza entre entidades podria adaptarse a esas caracteristicas y complejizar/mejorar estos modelos.

## Estructura de proyectos
***
proyecto/

├── install_packages.ipynb

├── data/

│   └── external/

│   │  ├── contaminados_processed_iva_representante_total_with_labels.csv

│   │   └── fuerza_IVA_representante.csv

│   └── processed/

│   │   └──  comunidades_louvain

│   │   │ ├── comunidades_resolution_study_louvain.csv

│   │   │ ├── estadistica_comunidades_louvain_with_iva_balance.csv

│   │   │ └── estadistica_comunidades_louvain_with_iva_balance.csv

│   │   └── comunidades_persona_natural

│   │   │  ├── comunidades_natural_sociedades.csv

│   │   │  ├── estadistica_comunidades_persona_natural_with_iva_balance.csv

│   │   │  └── estadistica_comunidades_persona_natural.csv

│   │   └──  malla_societaria_procesada

│   │   │  ├── anomalias_capital_sociedades.csv

│   │   │  └── sociedades_participacion_capital_nozero.csv

├── reports/

│   ├── evaluacion_grupos_economicos_louvain.ipynb

│   └── louvain_resolucion_performance.ipynb

└── src/

│  ├── algorithms/ 
    
│  │   ├── comunidades_louvain_algorithm_expanded.py

│  │   └── comunidades_sociedades_por_contribuyente.ipynb
    
│  ├── features/

│   │  ├── download_data.py
   
│  │   └── depuracion_sociedades.ipynb
   
│  └── notebooks/
   
│  │    ├── comunidades_louvain

│  │    │  ├── EDA_comunidades_louvain.ipynb

│  │    │  ├── IVA_comunidades_louvain.ipynb

│  │    │  └── louvain_resolucion_performance.ipynb
        
│  │    └── comunidades_persona_natural

│  │    │  ├── EDA_comunidades_persona_natural.ipynb

│  │    │  └── IVA_comunidades_persona_natural.ipynb




