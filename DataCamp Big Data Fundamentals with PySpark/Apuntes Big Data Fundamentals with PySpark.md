
# Capitulo 1. Fundamentos de BigData e Introduccion a Spark como un marco de trabajo distribuido de computacion
## 1. Introducción al Big Data

El **Big Data** se refiere al estudio y aplicación de conjuntos de datos tan grandes y complejos que el software de procesamiento de datos tradicional no puede manejarlos.

### Las 3 V's del Big Data

[![Imagen de](https://encrypted-tbn2.gstatic.com/licensed-image?q=tbn:ANd9GcSRfdpM66ZU_p3C4mMJiYrh-PT4wDTx1B_ash_AqVdc7Mzkwp2x2n1Qd_ptOd2VHH_qZVBhAJzwifmOydFDC8mpZ8W8vOLfmYgeMsI3upmdPpMrUWw)Se abre en una ventana nueva](https://encrypted-tbn2.gstatic.com/licensed-image?q=tbn:ANd9GcSRfdpM66ZU_p3C4mMJiYrh-PT4wDTx1B_ash_AqVdc7Mzkwp2x2n1Qd_ptOd2VHH_qZVBhAJzwifmOydFDC8mpZ8W8vOLfmYgeMsI3upmdPpMrUWw)

Shutterstock

Para definir Big Data, nos basamos en estas tres características principales:
1. **Volumen (Volume):** La cantidad de datos generados.
2. **Variedad (Variety):** Los diferentes tipos de fuentes y formatos (texto, video, logs, bases de datos, etc.).
3. **Velocidad (Velocity):** La rapidez con la que se generan y mueven los datos (en tus notas originales, "Speed" estaba bajo Volumen, pero en realidad define a la Velocidad).

### Terminología Clave de Computación

- **Clustered Computing (Computación en Clúster):** Colección de recursos de múltiples máquinas trabajando juntas.
- **Parallel Computing (Computación Paralela):** Computación simultánea en una sola computadora (usando múltiples núcleos).
- **Distributed Computing (Computación Distribuida):** Colección de nodos (computadoras en red) que ejecutan tareas en paralelo.
- **Batch Processing (Procesamiento por Lotes):** Dividir el trabajo en piezas pequeñas y ejecutarlas en máquinas individuales (típicamente con datos históricos).
- **Real-time Processing (Procesamiento en Tiempo Real):** Procesamiento inmediato de los datos según llegan.

---
## 2. Sistemas de Procesamiento: Hadoop vs. Spark

Aquí es fundamental entender la evolución tecnológica.
### Hadoop / MapReduce

Es un framework escalable y tolerante a fallos escrito en Java.
- **Open Source.**
- Enfocado principalmente en **Batch processing** (lotes). Escribe mucho en disco duro, lo que lo hace más lento para procesos iterativos.
### Apache Spark

Es un sistema de computación en clúster de propósito general y extremadamente rápido.

- **Open Source.**
- Soporta tanto **Batch** como **Real-time processing**.
- **Nota:** Hoy en día, Spark se prefiere sobre MapReduce por su velocidad y facilidad de uso.

|**Característica**|**Hadoop MapReduce**|**Apache Spark**|
|---|---|---|
|**Velocidad**|Lento (mucha escritura en disco)|Muy rápido (procesamiento en memoria)|
|**Uso**|Procesamiento por lotes|Lotes, Streaming, ML, SQL|

---

## 3. El Ecosistema de Apache Spark

### Características Principales

- Framework de computación distribuida en clúster.
- Eficiencia gracias a computaciones **in-memory** (en memoria RAM) para grandes sets de datos.
- Soporte multilenguaje: Java, Scala, Python, R y SQL.

### Componentes de Spark

[![Imagen de](https://encrypted-tbn1.gstatic.com/licensed-image?q=tbn:ANd9GcQI-tOCofZyfvkshnf-qqSbAk-5OC-aeuNM52dmJErVkcolOssN4aIMEnuoD8zRJC9rObYZ1aXZK0Lfh7EA-fdji9gGT11AfWKpcp-f1-Y9UTlG7qM)Se abre en una ventana nueva](https://encrypted-tbn1.gstatic.com/licensed-image?q=tbn:ANd9GcQI-tOCofZyfvkshnf-qqSbAk-5OC-aeuNM52dmJErVkcolOssN4aIMEnuoD8zRJC9rObYZ1aXZK0Lfh7EA-fdji9gGT11AfWKpcp-f1-Y9UTlG7qM)

Shutterstock

Spark no es solo una herramienta, es un conjunto de librerías construidas sobre un núcleo:

1. **Spark SQL:** Para trabajar con datos estructurados.
2. **MLlib:** Librería de Machine Learning.
3. **GraphX:** Procesamiento de grafos.
4. **Spark Streaming:** Procesamiento de datos en tiempo real.
5. **Spark Core / RDD API:** La base sobre la que funciona todo lo anterior.

### Modos de Despliegue

1. **Modo Local:** Tu propia máquina (laptop). Ideal para prototipar, testear y aprender.
2. **Modo Clúster:** Conjunto de máquinas definidas. Ideal para producción.
    - _Flujo de trabajo:_ Desarrollas en Local -> Despliegas en Clúster (sin cambiar el código).

---

## 4. PySpark: Spark con Python

Spark está escrito originalmente en **Scala**. Para permitir que los analistas de datos que usan Python trabajen con él, la comunidad creó **PySpark**.

- Tiene una velocidad de computación similar a Scala.
- Las APIs son muy parecidas a librerías que ya conoces como **Pandas** y **Scikit-learn**.

### Spark Shell

Es un entorno interactivo para ejecutar trabajos de Spark rápidamente (prototipado).

- `spark-shell`: Para Scala.
- `pyspark`: Para Python (permite interactuar con estructuras de datos de Spark).
- `SparkR`: Para R.

---

## 5. SparkContext: La puerta de entrada

El **SparkContext** (a menudo llamado `sc` en el código) es el punto de entrada al mundo Spark. Imagínalo como la "llave de la casa" que conecta tu código Python con el clúster.

**Inspeccionando el `sc`:**

- `sc.version`: Versión de Spark.
- `sc.pythonVer`: Versión de Python
- `sc.master`: URL del clúster o "local" si estás en tu máquina.

### Cargando datos (Creando RDDs)

Hay dos formas básicas de empezar a trabajar con datos desde el `sc`:

1. `sc.parallelize([1, 2, 3])`: Convierte una lista de Python en un RDD distribuido.
2. `sc.textFile("archivo.txt")`: Carga datos desde un archivo externo.

---
## 6. Funciones Lambda en Python (Esencial para Spark)

Spark usa mucho la programación funcional (map, filter), por lo que dominar las funciones **lambda** (anónimas) es vital.

### Sintaxis

La forma general es: `lambda argumentos: expresión`. No tienen nombre y retornan el resultado de la expresión automáticamente.

```python
# Función normal (def)
def cubo(x):
    return x ** 3

# Función Lambda equivalente
g = lambda x: x ** 3

print(g(10)) # Imprime 1000
```

### Map() y Filter()

Estas funciones toman una lista y le aplican una lógica a cada elemento.

**1. map(función, lista):** Aplica la función a _todos_ los elementos.


```Python
items = [1, 2, 3, 4]
# Suma 2 a cada número
resultado = list(map(lambda x: x + 2, items)) 
# Resultado: [3, 4, 5, 6]
```

**2. filter(función, lista):** Retorna solo los elementos donde la función es `True`._Nota:_ En tus notas originales había un error de sintaxis en esta parte. La forma correcta es:



```Python
items = [1, 2, 3, 4]
# Filtra números impares (donde el residuo de dividir por 2 no es 0)
resultado = list(filter(lambda x: x % 2 != 0, items))
# Resultado: [1, 3]
```

---

Para asegurarnos de que esta base ha quedado clara antes de pasar al Capítulo 2, te propongo una pequeña cuestión:

Si quisieras usar `sc.parallelize` para crear un set de datos con los números del 1 al 10, y luego quisieras quedarte **solo con los mayores de 5**, ¿cómo combinarías `filter` y una función `lambda` para hacerlo conceptualmente?

---


# Capitulo 2. Introduccion a RDDs, diferentes propiedades de RDDs, metodologias de creacion de RDDs y operaciones de RDD (Transformaciones y actiones)

## 1. Abstracción de Datos con RDDs

### ¿Qué son los RDDs?

RDD significa **Resilient Distributed Datasets** (Conjuntos de Datos Distribuidos Resilientes). Es la estructura de datos fundamental de Spark.

- **Resilient (Resiliente):** Capacidad de soportar fallos. Si un nodo del clúster cae, Spark puede reconstruir los datos perdidos gracias a su linaje.
- **Distributed (Distribuido):** Los datos se dividen y almacenan en múltiples máquinas.
- **Datasets:** Colección de datos particionados (Arrays, Tablas, Tuplas, etc.).

### Creación de RDDs

Existen dos formas principales de crear un RDD:

1. **Paralelizando** una colección existente (listas, arrays).
2. Desde **datasets externos** (HDFS, Amazon S3, archivos de texto).
3. Transformando **RDDs existentes**.

#### Código: `parallelize()`

Se usa para convertir listas de Python en RDDs.

```Python
numRDD = sc.parallelize([1, 2, 3, 4])
helloRDD = sc.parallelize("Hello world")

print(type(helloRDD)) 
# Salida esperada: <class 'pyspark.rdd.RDD'>
```

#### Código: `textFile()`

Se usa para cargar archivos externos.

```Python
# Nota: 'sc' (SparkContext) suele ir en minúsculas por convención
fileRDD = sc.textFile("README.md")
print(type(fileRDD))
```

### Entendiendo las Particiones

Una partición es una división lógica de un gran conjunto de datos distribuido. Es la unidad básica de paralelismo en Spark.

- Puedes definir el número mínimo de particiones con el parámetro `minPartitions`.

```Python
# Usamos range() en lugar de rango()
numRDD = sc.parallelize(range(10), minPartitions=6)

fileRDD = sc.textFile("README.md", minPartitions=6)
```

- Para verificar cuántas particiones tiene un RDD: `fileRDD.getNumPartitions()`

---

## 2. Transformaciones y Acciones Básicas

En Spark, las operaciones se dividen en dos tipos. Es vital entender la diferencia:

1. **Transformaciones:** Crean un _nuevo_ RDD a partir de uno existente. Son **Lazy (Perezosas)**; no se ejecutan hasta que se llama a una acción.
2. **Acciones:** Ejecutan la computación y devuelven un valor (o guardan datos) al driver o disco.

### Transformaciones Básicas

- **map():** Aplica una función a _cada_ elemento del RDD.

```Python
    RDD = sc.parallelize([1, 2, 3, 4])
    RDD_map = RDD.map(lambda x: x * x)
    # Resultado conceptual: [1, 4, 9, 16]
```

- **filter():** Retorna un nuevo RDD solo con los elementos que cumplen una condición (devuelven `True`).

```Python
    RDD = sc.parallelize([1, 2, 3, 4])
    RDD_filter = RDD.filter(lambda x: x > 2)
    # Resultado conceptual: [3, 4]
 ```
    
- **flatMap():** Similar a map, pero cada elemento de entrada puede mapearse a 0 o más elementos de salida (aplana el resultado).
   
    ```Python
    RDD = sc.parallelize(["Hello world", "How are you"])
    RDD_flatmap = RDD.flatMap(lambda x: x.split(" "))
    # Resultado conceptual: ["Hello", "world", "How", "are", "you"]
    ```

- **union():** Une dos RDDs.
    
    ```Python
    inputRDD = sc.textFile("logs.txt")
    errorRDD = inputRDD.filter(lambda x: "error" in x.split())
    warningsRDD = inputRDD.filter(lambda x: "warnings" in x.split())
    
    combinedRDD = errorRDD.union(warningsRDD)
    ```

### Acciones Básicas

- **collect():** Devuelve todos los elementos del RDD al nodo conductor (Cuidado: ¡puede desbordar la memoria si el RDD es enorme!).
- **take(N):** Devuelve los primeros N elementos.
- **first():** Devuelve el primer elemento.
- **count():** Cuenta el número total de elementos.

---

## 3. Pair RDDs (Clave/Valor)

Los datasets de la vida real suelen ser pares de **Clave/Valor** (Key/Value). Spark tiene operaciones especiales para este tipo de datos.

- **Estructura:** Tuplas de Python `(Clave, Valor)`.
### Creación de Pair RDDs

```Python
# Desde una lista de tuplas
my_tuple = [('Sam', 23), ('Mary', 34), ('Peter', 25)]
pairRDD_tuple = sc.parallelize(my_tuple)

# Desde un RDD regular (usando map para crear tuplas)
regularRDD = sc.parallelize(["Sam 23", "Mary 34", "Peter 25"])
pairRDD_RDD = regularRDD.map(lambda s: (s.split(' ')[0], s.split(' ')[1]))
```

### Transformaciones en Pair RDDs

- **reduceByKey(func):** Combina valores con la misma clave. Es muy eficiente porque realiza una combinación parcial en cada nodo antes de enviar datos a través de la red.
    
    ```Python
    regularRDD = sc.parallelize([('Messi', 23), ('Ronaldo', 34), ('Neymar', 22), ('Messi', 24)])
    
    # Suma los valores de las claves iguales (ej. Messi: 23 + 24 = 47)
    pairRDD_reduce = regularRDD.reduceByKey(lambda x, y: x + y)
    pairRDD_reduce.collect()
    ```

- **sortByKey():** Ordena el RDD por su clave.
    
    ```Python
    # Invertimos (Valor, Clave) para ordenar, y ordenamos descendente
    pairRDD_rev = pairRDD_reduce.map(lambda x: (x[1], x[0]))
    pairRDD_rev.sortByKey(ascending=False).collect()
    ```

- **groupByKey():** Agrupa todos los valores de la misma clave en un iterable.

    - _Nota:_ `groupByKey` puede causar problemas de memoria si una clave tiene demasiados valores. Se prefiere `reduceByKey` cuando es posible.    
    
    ```Python
    airports = [('US', 'JFK'), ('UK', 'LHR'), ('FR', 'CDG'), ('US', 'SFO')]
    rdd_air = sc.parallelize(airports)
    pairRDD_group = rdd_air.groupByKey().collect()
    
    for pais, aeropuertos in pairRDD_group:
        print(pais, list(aeropuertos))
    # Salida: US ['JFK', 'SFO'], UK ['LHR']...
    ```
    
- **join():** Une dos RDDs basándose en sus claves.
    
    Python
    
    ```
    # (Nombre, Edad)
    RDD1 = sc.parallelize([('Messi', 34), ('Ronaldo', 32)])
    # (Nombre, Goles)
    RDD2 = sc.parallelize([('Messi', 100), ('Ronaldo', 80)])
    
    RDD1.join(RDD2).collect()
    # Resultado: [('Messi', (34, 100)), ('Ronaldo', (32, 80))]
    ```

### Acciones en Pair RDDs

- **countByKey():** Cuenta elementos por clave. Devuelve un diccionario al driver.
- **collectAsMap():** Devuelve todo el RDD como un diccionario de Python (Map).

---

## 4. Acciones Avanzadas de RDD

- **reduce(func):** Agrega todos los elementos del RDD usando una función (debe ser conmutativa y asociativa).
    
    ```Python
    x = [1, 3, 4, 6]
    RDD = sc.parallelize(x)
    # Suma todos los números: 1+3+4+6 = 14
    RDD.reduce(lambda x, y : x + y)    ```
    
- **saveAsTextFile():** Guarda el RDD en disco. Crea un directorio y guarda cada partición como un archivo separado (`part-00000`, `part-00001`...).
- **coalesce(N):** Reduce el número de particiones. Útil antes de guardar un archivo si quieres menos archivos de salida.
    
    ```Python
    # Reduce a 1 partición y guarda (creará un solo archivo de texto)
    RDD.coalesce(1).saveAsTextFile("tempFile")
    ```

---

## 5. Introducción a DataFrames

Los RDDs son potentes pero de "bajo nivel". Los **DataFrames** son la evolución moderna en Spark.

### ¿Qué es un DataFrame?

Es una colección distribuida de datos organizados en **columnas con nombre**.

- Similar a una tabla en una base de datos relacional o un DataFrame de Pandas.
- Soporta esquemas (Tipos de datos: String, Integer, etc.).
- Optimizado automáticamente por Spark (Catalyst Optimizer), lo que los hace más rápidos que los RDDs en Python.

### SparkSession

Es el punto de entrada único para trabajar con DataFrames (reemplaza la necesidad de tener `sc` y `sqlContext` separados).

- Variable común: `spark`.

### Creación de DataFrames

1. **Desde un RDD (con esquema):**

    ```Python
    # Datos
    iphones_RDD = sc.parallelize([("XS", 2018, 5.8, 1000), ("XR", 2018, 6.1, 750)])
    # Nombres de columnas
    names = ["Model", "Year", "ScreenSize", "Price"]
    
    iphones_df = spark.createDataFrame(iphones_RDD, schema=names)
    ```

2. **Desde fuentes de datos (CSV, JSON, TXT):**
    
    ```Python
    # inferSchema=True le dice a Spark que adivine los tipos de datos (int vs string)
    # header=True usa la primera fila como nombres de columna
    df_csv = spark.read.csv("people.csv", header=True, inferSchema=True)
    
    df_json = spark.read.json("people.json")
    ```

### Operaciones Básicas en DataFrames

Las operaciones se parecen mucho a SQL o Pandas.

- **select():** Selecciona columnas específicas.
- **filter():** Filtra filas (equivalente al `WHERE` de SQL)
- **groupBy() + count():** Agrupa y cuenta.
- **orderBy():** Ordena.
- **dropDuplicates():** Elimina filas duplicadas.
- **withColumnRenamed():** Renombra una columna.
- **show():** Acción para mostrar las primeras 20 filas en formato tabla.
- **printSchema():** Muestra la estructura (nombres de columna y tipos de datos).

---

Esta sección cubre mucho terreno técnico. Hay un concepto clave en el apartado de **Pair RDDs** sobre el que me gustaría preguntarte para ver si quedó claro:

¿Podrías decirme cuál es la diferencia principal entre **`groupByKey`** y **`reduceByKey`** y por qué generalmente preferimos usar `reduceByKey` si queremos sumar valores?

---


# Capítulo 3. Introduccion a Spark SQL, abstraccion de dataframes, creacion, operaciones y visualizacion de BigData a partir de Dataframes

## 1. Interacción con Spark SQL

Spark nos ofrece dos "idiomas" para hablar con los datos. Puedes elegir el que mejor se adapte a tu problema o a tu experiencia previa.

### API de DataFrames vs. Consultas SQL

1. **DataFrame API (Lenguaje de Dominio Específico - DSL):**
    - Es programático. Construyes las consultas encadenando métodos (`.select()`, `.filter()`).
    - Permite comprobación de errores en tiempo de compilación (en lenguajes tipados, aunque menos en Python).
    - A menudo es más fácil de construir dinámicamente dentro de scripts de Python.
        
2. **Consultas SQL:**
    - Usa el estándar SQL (ANSI SQL).
    - Es conciso, legible y portable (si sabes SQL, ya sabes usar Spark SQL).
    - **Importante:** Internamente, Spark convierte ambas formas (API y SQL) al mismo plan de ejecución optimizado (Catalyst Optimizer), por lo que **no hay diferencia de rendimiento**.

---

## 2. Ejecutando Consultas SQL en PySpark

Para usar SQL puro, primero debemos decirle a Spark que trate nuestro DataFrame como si fuera una tabla de base de datos.

### El método `createOrReplaceTempView`

Este paso es obligatorio si quieres usar `spark.sql()`. Crea una "vista temporal" en memoria que dura lo que dure tu sesión de Spark.

```Python
# 1. Registramos el DataFrame como una vista temporal llamada "table1"
df.createOrReplaceTempView("table1")

# 2. Ejecutamos SQL estándar
df2 = spark.sql("SELECT col1, col2 FROM table1")

# 3. El resultado sigue siendo un DataFrame distribuido
df2.collect()
```

### Ejemplos de Consultas Comunes

**A. Extracción Básica (SELECT)**

```Python
test_df.createOrReplaceTempView("test_table")

# Definimos la consulta como un string
query = "SELECT Product_ID FROM test_table"

# Ejecutamos
test_product_df = spark.sql(query)
test_product_df.show(5)
```

**B. Agregación y Agrupamiento (GROUP BY)**

```Python
test_df.createOrReplaceTempView("test_table")

# Calculamos el máximo de compra por edad
query = "SELECT Age, max(Purchase) FROM test_table GROUP BY Age"

spark.sql(query).show()
```

C. Filtrado (WHERE)

Nota: En SQL estándar se usa un solo igual = o la palabra IN para comparaciones, aunque Spark SQL suele ser permisivo con sintaxis tipo Python (==). Lo correcto en SQL es:

```Python
test_df.createOrReplaceTempView("test_table")

# Filtramos donde el género es Femenino
query = "SELECT Age, Purchase, Gender FROM test_table WHERE Gender = 'Female'"

spark.sql(query).show(5)
```

---

## 3. Visualización de Datos en Big Data

Visualizar Big Data es un desafío. **No puedes graficar mil millones de puntos** en una pantalla; se vería como una mancha sólida y tu ordenador se bloquearía intentando renderizarlo.

**Estrategia general:**

1. **Agregar:** Calcular medias, sumas o conteos en Spark (distribuido).
2. **Muestrear:** Tomar una muestra representativa pequeña.
3. **Graficar:** Traer esos datos reducidos al driver y graficar con herramientas locales.

### Herramientas de Visualización

Las librerías estándar de Python (Matplotlib, Seaborn, Bokeh) están diseñadas para trabajar con datos en la memoria de una sola máquina, no con clústeres distribuidos. Por eso necesitamos intermediarios.

#### Método 1: Librería `pyspark_dist_explore`

Es una librería diseñada para facilitar la visualización rápida directamente sobre DataFrames de Spark. (Nota: en tu texto decía "explote", lo he corregido).

- Funciones principales: `hist()`, `distplot()`, `pandas_histogram()`.

```Python
# Requiere instalar pyspark_dist_explore y matplotlib
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt

test_df = spark.read.csv("test.csv", header=True, inferSchema=True)

# Seleccionamos una columna numérica
test_df_age = test_df.select('Age')

# Crea el histograma distribuyendo el cálculo
hist(test_df_age, bins=20, color="red")
plt.show()
```

#### Método 2: Conversión a Pandas (`toPandas`)

Es el método más común pero también el más **peligroso**.

- Convierte el DataFrame distribuido de Spark en un DataFrame local de Pandas (en memoria RAM).

> **⚠️ ADVERTENCIA CRÍTICA:** Solo usa `toPandas()` cuando el resultado sea pequeño (ej. después de un `groupBy` o un `limit`). Si intentas hacer `toPandas()` de un dataset de 100GB, tu programa fallará por "Out of Memory" (OOM), ya que intenta meter todo en la RAM de tu ordenador local.


```Python
test_df = spark.read.csv("test.csv", header=True, inferSchema=True)

# Hacemos una muestra o agregación antes
test_df_sample_pandas = test_df.limit(1000).toPandas()

# Ahora usamos la API de gráficos de Pandas/Matplotlib
test_df_sample_pandas['Age'].hist()
```

#### Método 3: HandySpark

Es una librería "wrapper" (envoltorio) que hace que los DataFrames de Spark se sientan como si fueran de Pandas, manteniendo el cómputo distribuido cuando es posible.

```Python
# Requiere: pip install handyspark
from handyspark import *

test_df = spark.read.csv("test.csv", header=True, inferSchema=True)

# Convertimos a HandyFrame
hdf = test_df.toHandy()

# Graficamos directamente
hdf.cols["Age"].hist()
```

---

## 4. Comparativa: Pandas vs. PySpark DataFrame

Es fundamental entender cuándo usar cada uno.

|**Característica**|**Pandas DataFrame**|**PySpark DataFrame**|
|---|---|---|
|**Ubicación de Datos**|**In-memory** (RAM de una sola máquina).|**Distribuido** (a través de múltiples nodos del clúster).|
|**Evaluación**|**Eager (Ansiosa):** El resultado se genera inmediatamente al ejecutar la línea.|**Lazy (Perezosa):** Solo se construye un plan de ejecución (DAG). Nada sucede hasta que llamas a una Acción.|
|**Mutabilidad**|**Mutable:** Puedes cambiar valores in-situ.|**Inmutable:** No puedes cambiar un RDD/DF original; creas uno nuevo transformado.|
|**Capacidad**|Limitada por la RAM de tu servidor.|Limitada por el tamaño del clúster (puede escalar a Petabytes).|
|**Operaciones**|API muy rica y madura para estadística compleja.|API enfocada en transformaciones de datos masivos y SQL.|

---

Con esto cerramos los fundamentos de Spark SQL y visualización.

Tengo una pregunta para verificar que el concepto de "Lazy Evaluation" (Evaluación Perezosa) y la visualización ha quedado claro:

Si ejecutas el código: `df.select("Age").filter("Age > 20")` y luego intentas graficarlo inmediatamente sin hacer un `collect()` o `toPandas()`, ¿por qué fallaría o no mostraría nada una librería como **Matplotlib** tradicional?

---

# Capítulo 4: Introduccion a Spark MLlib, las tres C's del aprendizaje automatico (Collaborative filtering, Classification and Clustering)

## 1. Introducción a PySpark MLlib

### ¿Qué es MLlib?

Es el componente de Apache Spark dedicado al Machine Learning (Aprendizaje Automático). Permite escalar algoritmos tradicionales para que funcionen con Big Data.

Ofrece herramientas para:

- **Algoritmos de ML:** Clasificación, regresión, clustering y filtrado colaborativo.
- **Featurization:** Extracción, transformación y reducción de dimensionalidad de características.
- **Pipelines:** Herramientas para construir y evaluar flujos de trabajo de ML.

### ¿Por qué usar PySpark MLlib en lugar de Scikit-learn?

- **Scikit-learn:** Excelente librería, pero diseñada para ejecutarse en **una sola máquina**. Si tus datos no caben en la memoria RAM de tu ordenador, Scikit-learn fallará.
    
- **Spark MLlib:** Diseñada para procesamiento **paralelo en un clúster**.
    - Soporta Java, Scala, Python y R.
    - Utiliza iteraciones en memoria (mucho más rápido que MapReduce en disco).

---

## 2. Las 3 C's de MLlib

Los algoritmos principales se pueden categorizar en:

1. **Collaborative Filtering (Filtrado Colaborativo):** Motores de recomendación (ej. "Usuarios que vieron esto también vieron...").
2. **Classification (Clasificación):** Identificar a qué categoría pertenece una observación (ej. "¿Es este email Spam o No Spam?"). Incluye Regresión para valores continuos.
3. **Clustering (Agrupamiento):** Agrupar datos basados en características similares sin etiquetas previas (No supervisado).

**Importaciones Clave:**

```Python
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.clustering import KMeans
```

---

## 3. Filtrado Colaborativo (Recomendadores)

El objetivo es llenar los huecos en una matriz de usuario-producto (predecir qué rating le daría un usuario a un producto que aún no ha visto).

### La clase `Rating`

Es un envoltorio (wrapper) para la tupla `(usuario, producto, puntuación)`.

```Python
from pyspark.mllib.recommendation import Rating

# Usuario 1, Producto 2, Rating 5.0
r = Rating(user=1, product=2, rating=5.0)
print(r[0], r[1], r[2])
```

### Preparación de Datos: `randomSplit`

Para evaluar si un modelo es bueno, nunca debemos probarlo con los mismos datos con los que lo entrenamos.

- **Training (Entrenamiento):** Para que el modelo aprenda.
- **Test (Prueba):** Para ver si acierta con datos nuevos.

```Python
data = sc.parallelize(range(1, 11)) # Datos del 1 al 10
# Divide 60% para training y 40% para test
training, test = data.randomSplit([0.6, 0.4])
```

### Algoritmo ALS (Alternating Least Squares)

Es el algoritmo estrella de Spark para recomendaciones.

```Python
# Datos de ejemplo: (Usuario, Producto, Rating)
r1 = Rating(1, 1, 1.0)
r2 = Rating(1, 2, 2.0)
r3 = Rating(2, 1, 2.0)

ratings = sc.parallelize([r1, r2, r3])

# Entrenamos el modelo
# rank: número de características latentes
# iterations: número de pasadas
model = ALS.train(ratings, rank=10, iterations=10)
```

### Predicción y Evaluación (Corrección Importante)

Para evaluar, usamos el MSE (Mean Squared Error).

Nota: He corregido la lógica del código original. Para calcular el error, necesitas unir (JOIN) la predicción con el dato real basándote en el Usuario y Producto.

```Python
# 1. Preparamos datos para predecir (solo usuario y producto, sin el rating real)
input_para_prediccion = ratings.map(lambda x: (x[0], x[1]))

# 2. Predecimos
predictions = model.predictAll(input_para_prediccion)

# 3. Preparamos RDDs clave-valor para hacer el Join: ((User, Product), Rating)
rates_kv = ratings.map(lambda x: ((x[0], x[1]), x[2]))
preds_kv = predictions.map(lambda x: ((x[0], x[1]), x[2]))

# 4. Unimos Realidad con Predicción
rates_preds = rates_kv.join(preds_kv)

# 5. Calculamos MSE: Promedio de (Real - Predicho)^2
MSE = rates_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

print("Mean Squared Error:", MSE)
```

---

## 4. Clasificación

Aprendizaje supervisado donde etiquetamos datos en clases (Binaria o Multiclase).

### Tipos de Datos: Vectores y LabeledPoint

MLlib basado en RDDs requiere estructuras de datos muy específicas.

1. **Vectores:** Representan las características (features).
    - _Dense (Denso):_ Array normal `[1.0, 2.0, 0.0]`.
    - _Sparse (Disperso):_ Ahorra memoria guardando solo índices con valores distintos de cero. Útil cuando tienes millones de columnas (ej. palabras en un texto).
    
    ```Python
    from pyspark.mllib.linalg import Vectors
    denseVec = Vectors.dense([1.0, 2.0, 3.0])
    # Tamaño 4, en el índice 1 hay un 1.0, en el índice 3 hay un 5.5
    sparseVec = Vectors.sparse(4, {1: 1.0, 3: 5.5}) 
    ```
    
1. **LabeledPoint:** Combina la etiqueta (respuesta correcta) con las características.
    - Etiqueta 0.0: Negativo.
    - Etiqueta 1.0: Positivo.

### Regresión Logística


```Python
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

data = [
    LabeledPoint(0.0, [0.0, 1.0]), # Clase 0
    LabeledPoint(1.0, [1.0, 0.0])  # Clase 1
]
RDD = sc.parallelize(data)

# Entrenamiento
lrm = LogisticRegressionWithLBFGS.train(RDD)

# Predicción
print(lrm.predict([1.0, 0.0])) # Debería predecir 1
```

---
## 5. Clustering (Agrupamiento)

Aprendizaje no supervisado. El algoritmo más común es **K-Means**.

### K-Means en MLlib

El objetivo es encontrar `K` centros y agrupar los puntos más cercanos a cada centro.

![Imagen de K-means clustering process](https://encrypted-tbn2.gstatic.com/licensed-image?q=tbn:ANd9GcT4Yxjx2NtZoKrH49_G6eLmaLtonsrS5teLTB5h-4VXMlHZregArFMBIf0bm-heglnsvrJiv9YMKQg7wQykRSzpwkDrU8oYHK2qU5MuIHIDXHymIYA)



```Python
# Carga y limpieza de datos
RDD = sc.textFile("WineData.csv") \
        .map(lambda x: x.split(",")) \
        .map(lambda x: [float(x[0]), float(x[1])])

# Entrenamiento del modelo
from pyspark.mllib.clustering import KMeans

# k=2 (queremos 2 grupos), maxIterations=10
model = KMeans.train(RDD, k=2, maxIterations=10)

# Ver los centros calculados
print(model.clusterCenters)
```

### Evaluación del Modelo (WSSSE)

En Clustering no tenemos "respuestas correctas", así que medimos la **Suma de los Errores Cuadráticos Dentro del Conjunto (WSSSE)**. Básicamente: ¿Qué tan compactos son mis grupos? Cuanto menor sea el número, más apretados están los grupos.


```Python
from math import sqrt

def error(point):
    center = model.centers[model.predict(point)]
    # Distancia euclidiana entre el punto y su centro
    return sqrt(sum([(x - y)**2 for x, y in zip(point, center)]))

WSSSE = RDD.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
```

### Visualización

Para visualizar, nuevamente usamos la estrategia de convertir resultados pequeños a Pandas.

```Python
import pandas as pd
import matplotlib.pyplot as plt

# Convertimos los datos originales a Pandas para graficar (si no son gigantes)
wine_data_pd = spark.createDataFrame(RDD, schema=["col1", "col2"]).toPandas()

# Convertimos los centros de los clusters a Pandas
cluster_centers_pd = pd.DataFrame(model.clusterCenters, columns=["col1", "col2"])

# Graficamos los puntos
plt.scatter(wine_data_pd["col1"], wine_data_pd["col2"], c='blue', label='Datos')
# Graficamos los centros encima
plt.scatter(cluster_centers_pd["col1"], cluster_centers_pd["col2"], color="red", marker="x", s=200, label='Centros')
plt.legend()
plt.show()
```

---

Como verás, trabajar con `mllib` (RDDs) es un poco más "manual" (tienes que crear tuplas, calcular errores con funciones lambda, etc.) comparado con las librerías modernas de DataFrames, pero es excelente para entender qué ocurre bajo el capó.
