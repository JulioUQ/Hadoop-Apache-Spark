# Actividad BATCH

## Sistema de ficheros HDFS y extracción de conocimiento de fuentes de datos heterogéneas mediante RDDs

El comando `!hdfs dfs -ls` / lista el contenido del directorio raíz de HDFS.

```python
!hdfs dfs -ls /
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> Found 20 items
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-22 16:13 /alluxio
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-21 11:29 /amshbase
> drwxrwxrwt   - yarn   hadoop          0 2025-10-22 23:46 /app-logs
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-22 12:45 /apps
> drwxr-xr-x   - yarn   hadoop          0 2025-07-21 11:32 /ats
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-21 11:32 /atsv2
> drwxr-xr-x   - sgraul hdfs            0 2025-09-24 13:49 /aula_M2.858
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-21 11:32 /bigtop
> drwxrwxrwx   - flink  hadoop          0 2025-07-22 16:32 /completed-jobs
> drwxr-xr-x   - yarn   hdfs            0 2025-07-21 14:24 /hadoop
> drwxr-xr-x   - hbase  hdfs            0 2025-10-22 12:01 /hbase
> drwx------   - livy   hdfs            0 2025-07-22 16:11 /livy-recovery
> drwxr-xr-x   - mapred hdfs            0 2025-07-22 14:54 /mapred
> drwxrwxrwx   - mapred hadoop          0 2025-07-22 15:22 /mr-history
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-21 11:22 /ranger
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-21 11:32 /services
> drwxrwxrwx   - spark  hadoop          0 2025-11-02 11:17 /spark-history
> drwxrwxrwx   - hdfs   hdfs            0 2025-10-19 13:31 /tmp
> drwxr-xr-x   - hdfs   hdfs            0 2025-10-30 10:54 /user
> drwxr-xr-x   - hdfs   hdfs            0 2025-07-22 14:53 /warehouse

La salida muestra 20 elementos, cada uno con:

- **Permisos** (ej. `drwxr-xr-x`)
- **Propietario y grupo** (ej. `hdfs hdfs`)
- **Tamaño** (0) porque son directorios
- **Fecha y hora** de modificación
- **Ruta** completa del directorio

Por ejemplo:
`/user` → contiene los espacios personales de cada usuario en HDFS.
`spark-history`, `mr-history`, `hbase`, etc. → directorios del ecosistema Hadoop y sus servicios asociados.

### **Ejercicio 1**: Gestión y análisis de datos en HDFS (_0.5 puntos_)

```python
# 1️.1. Verificar información general del archivo
!hdfs dfs -ls /aula_M2.858/data/consumo_hogar_2024.csv
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> -rw-r--r--   3 martam hdfs        711 2025-10-20 09:28 /aula_M2.858/data/consumo_hogar_2024.csv

```python
# 1.2. Mostrar detalles técnicos (tamaño, factor de replicación, bloques, propietario, etc.)
!hdfs fsck /aula_M2.858/data/consumo_hogar_2024.csv -files -blocks -locations
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> Connecting to namenode via [http://eimtcld3node1:50070/fsck?ugi=jubedaq&files=1&blocks=1&locations=1&path=%2Faula_M2.858%2Fdata%2Fconsumo_hogar_2024.csv](http://eimtcld3node1:50070/fsck?ugi=jubedaq&files=1&blocks=1&locations=1&path=%2Faula_M2.858%2Fdata%2Fconsumo_hogar_2024.csv)
> FSCK started by jubedaq (auth:SIMPLE) from /172.17.58.200 for path /aula_M2.858/data/consumo_hogar_2024.csv at Sun Nov 02 12:32:04 CET 2025
> 
> /aula_M2.858/data/consumo_hogar_2024.csv 711 bytes, replicated: replication=3, 1 block(s):  OK
> 0. BP-1495504404-172.17.58.6-1753086652608:blk_1073760312_19539 len=711 Live_repl=3  [DatanodeInfoWithStorage[172.17.58.200:50010,DS-269ffbda-b619-40ff-8bb8-b8d6d7acfd8a,DISK], DatanodeInfoWithStorage[172.17.58.201:50010,DS-def0af19-c2a5-4be3-bffe-a4a1ace3bfe6,DISK], DatanodeInfoWithStorage[172.17.58.6:50010,DS-94d61690-8bae-419c-8424-c5e44bf76713,DISK]]
> 

> Status: HEALTHY
>  Number of data-nodes:	3
>  Number of racks:		1
>  Total dirs:			0
>  Total symlinks:		0
> 
> Replicated Blocks:
>  Total size:	711 B
>  Total files:	1
>  Total blocks (validated):	1 (avg. block size 711 B)
>  Minimally replicated blocks:	1 (100.0 %)
>  Over-replicated blocks:	0 (0.0 %)
>  Under-replicated blocks:	0 (0.0 %)
>  Mis-replicated blocks:		0 (0.0 %)
>  Default replication factor:	3
>  Average block replication:	3.0
>  Missing blocks:		0
>  Corrupt blocks:		0
>  Missing replicas:		0 (0.0 %)
>  Blocks queued for replication:	0
> 
> Erasure Coded Block Groups:
>  Total size:	0 B
>  Total files:	0
>  Total block groups (validated):	0
>  Minimally erasure-coded block groups:	0
>  Over-erasure-coded block groups:	0
>  Under-erasure-coded block groups:	0
>  Unsatisfactory placement block groups:	0
>  Average block group size:	0.0
>  Missing block groups:		0
>  Corrupt block groups:		0
>  Missing internal blocks:	0
>  Blocks queued for replication:	0
> FSCK ended at Sun Nov 02 12:32:04 CET 2025 in 3 milliseconds
> 
> 
> The filesystem under path '/aula_M2.858/data/consumo_hogar_2024.csv' is HEALTHY

```python
# 1.3. Comprobar que no haya bloques dañados
!hdfs fsck /aula_M2.858/data/consumo_hogar_2024.csv -blocks -locations -racks
```
> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> Connecting to namenode via [http://eimtcld3node1:50070/fsck?ugi=jubedaq&blocks=1&locations=1&racks=1&path=%2Faula_M2.858%2Fdata%2Fconsumo_hogar_2024.csv](http://eimtcld3node1:50070/fsck?ugi=jubedaq&blocks=1&locations=1&racks=1&path=%2Faula_M2.858%2Fdata%2Fconsumo_hogar_2024.csv)
> FSCK started by jubedaq (auth:SIMPLE) from /172.17.58.200 for path /aula_M2.858/data/consumo_hogar_2024.csv at Sun Nov 02 12:32:52 CET 2025
> 
> 
> Status: HEALTHY
>  Number of data-nodes:	3
>  Number of racks:		1
>  Total dirs:			0
>  Total symlinks:		0
> 
> Replicated Blocks:
>  Total size:	711 B
>  Total files:	1
>  Total blocks (validated):	1 (avg. block size 711 B)
>  Minimally replicated blocks:	1 (100.0 %)
>  Over-replicated blocks:	0 (0.0 %)
>  Under-replicated blocks:	0 (0.0 %)
>  Mis-replicated blocks:		0 (0.0 %)
>  Default replication factor:	3
>  Average block replication:	3.0
>  Missing blocks:		0
>  Corrupt blocks:		0
>  Missing replicas:		0 (0.0 %)
>  Blocks queued for replication:	0
> 
> Erasure Coded Block Groups:
>  Total size:	0 B
>  Total files:	0
>  Total block groups (validated):	0
>  Minimally erasure-coded block groups:	0
>  Over-erasure-coded block groups:	0
>  Under-erasure-coded block groups:	0
>  Unsatisfactory placement block groups:	0
>  Average block group size:	0.0
>  Missing block groups:		0
>  Corrupt block groups:		0
>  Missing internal blocks:	0
>  Blocks queued for replication:	0
> FSCK ended at Sun Nov 02 12:32:52 CET 2025 in 2 milliseconds
> 
> 
> The filesystem under path '/aula_M2.858/data/consumo_hogar_2024.csv' is HEALTHY


```python
# 1.4. Ver una muestra de los datos (sin descargarlo completo)
!hdfs dfs -head /aula_M2.858/data/consumo_hogar_2024.csv | head -n 10
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> ﻿fecha,hora,id_hogar,consumo_kwh,temperatura_ext,region
> 2024-01-01,00:00,H001,1.23,8.2,Madrid
> 2024-01-01,01:00,H001,1.15,7.9,Madrid
> 2024-01-01,00:00,H002,0.95,9.1,Barcelona
> 2024-01-01,01:00,H002,0.88,8.7,Barcelona
> 2024-01-02,00:00,H001,1.35,7.5,Madrid
> 2024-01-02,01:00,H001,1.20,7.2,Madrid
> 2024-01-02,00:00,H002,1.05,8.3,Barcelona
> 2024-01-02,01:00,H002,0.97,7.8,Barcelona
> 2024-02-01,12:00,H003,2.10,15.2,Sevilla


```python
# 1.5. Crear tu carpeta personal “procesado”
!hdfs dfs -mkdir -p /user/jubedaq/procesado
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]


```python
# 1.6. Copiar y renombrar el archivo validado
!hdfs dfs -cp /aula_M2.858/data/consumo_hogar_2024.csv /user/jubedaq/procesado/consumo_hogar_2024_validado.csv
```

SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]

```python
# 1.7. Crear informe de verificación (texto plano) --> Ejecutar en la terminal
# !echo "INFORME DE VERIFICACIÓN - consumo_hogar_2024.csv
# ------------------------------------------------------
# Propietario original: martam
# Verificado por: jubedaq
# Tamaño: 711 bytes
# Factor de replicación: 3
# Número de bloques: 1
# Estado del sistema de archivos: HEALTHY (sin bloques dañados)
# Número de data-nodes: 3
# Número de racks: 1
# Ruta origen: /aula_M2.858/data/consumo_hogar_2024.csv
# Ruta destino: /user/jubedaq/procesado/consumo_hogar_2024_validado.csv
# Fecha de validación: Sun Nov 02 12:32:52 CET 2025
# ------------------------------------------------------
# Observaciones:
# El archivo fue leído correctamente. La estructura CSV contiene cabecera y registros con
# campos: fecha, hora, id_hogar, consumo_kwh, temperatura_ext, region.
# No se detectaron errores de integridad ni bloques corruptos.
# ------------------------------------------------------" > informe_validacion.txt
```

```python
# 1.8. Subir el informe al HDFS
!hdfs dfs -put informe_validacion.txt /user/jubedaq/procesado/
```
> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]


```python
# 1.9. Verificar que el informe se subio a la carpeta HDFS de mi usuario
!hdfs dfs -ls /user/jubedaq/procesado/
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> Found 2 items
> -rw-r--r--   3 jubedaq students        711 2025-11-02 12:34 /user/jubedaq/procesado/consumo_hogar_2024_validado.csv
> -rw-r--r--   3 jubedaq students        852 2025-11-02 12:45 /user/jubedaq/procesado/informe_validacion.txt

```python
# 1.10 Verificacion visual del contenido del informe
!hdfs dfs -cat /user/jubedaq/procesado/informe_validacion.txt
```

> SLF4J: Class path contains multiple SLF4J bindings.
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/hadoop/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: Found binding in [jar:file:/usr/bigtop/3.3.0/usr/lib/tez/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
> SLF4J: See [http://www.slf4j.org/codes.html#multiple_bindings](http://www.slf4j.org/codes.html#multiple_bindings) for an explanation.
> SLF4J: Actual binding is of type [org.slf4j.impl.Reload4jLoggerFactory]
> INFORME DE VERIFICACIÓN - consumo_hogar_2024.csv
> ------------------------------------------------------
> Propietario original: martam
> Verificado por: jubedaq
> Tamaño: 711 bytes
> Factor de replicación: 3
> Número de bloques: 1
> Estado del sistema de archivos: HEALTHY (sin bloques dañados)
> Número de data-nodes: 3
> Número de racks: 1
> Ruta origen: /aula_M2.858/data/consumo_hogar_2024.csv
> Ruta destino: /user/jubedaq/procesado/consumo_hogar_2024_validado.csv
> Fecha de validación: Sun Nov 02 12:32:52 CET 2025
> ------------------------------------------------------
> Observaciones:
> El archivo fue leído correctamente. La estructura CSV contiene cabecera y registros con
> campos: fecha, hora, id_hogar, consumo_kwh, temperatura_ext, region.
> No se detectaron errores de integridad ni bloques corruptos.
> ------------------------------------------------------

# **Apache Spark RDDs (Resilient Distributed Datasets)**

En el marco del procesamiento de grandes volúmenes de datos con Apache Spark, los RDDs, o Resilient Distributed Datasets, juegan un papel fundamental. Un RDD es una colección de elementos que se distribuyen a través de un clúster de nodos y sobre la cual se pueden aplicar operaciones que se ejecutan en paralelo.

Recordemos sus características:

- Inmutabilidad: Una vez que se crea un RDD, no se puede modificar. En lugar de eso, cualquier operación que modifique los datos generará un nuevo RDD.

- Distribución: Los RDDs están repartidos entre los diferentes nodos del clúster, permitiendo un procesamiento paralelo eficiente.

- Tolerancia a Fallos: Los RDDs son resistentes a fallos. En caso de que un nodo falle, Spark puede reconstruir los datos perdidos a partir de los datos originales y las operaciones realizadas.

Esta estructura permite un procesamiento eficiente y escalable de datos, lo que es esencial para trabajar con grandes volúmenes de información en entornos de clúster.

A continuación se muestra el código que debéis ejecutar para configurar vuestro entorno de Spark.

> Como referencia a todos métodos que se requieren para implementar esta práctica podéis consultar:
> * [API Python de Spark](https://archive.apache.org/dist/spark/docs/2.4.0/api/python/index.html)

### Configuración del entorno python + spark

```python
import findspark
import os

SPARK_HOME_PATH = "/usr/bigtop/current/spark-client/" 
os.environ['SPARK_HOME'] = SPARK_HOME_PATH
findspark.init(SPARK_HOME_PATH)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ActividadRDDs_usuario") \
    .master("local[*]") \
    .getOrCreate()

print(spark.sparkContext.appName)
print(spark.version)

sc=spark.sparkContext
```

> ActividadRDDs_usuario
> 3.3.4

### **Ejercicio 2**: Manipulación de RDDs en PySpark (*1.25 puntos*)

En este ejercicio, te proporcionamos dos listas de números en las que realizarás diversas operaciones sobre ellas utilizando RDDs en PySpark. La solución y el enfoque quedan a tu criterio.
Contexto:

Tienes dos listas de números que representan datos de sensores:
- **Sensor A**: Números del 1 al 25.
- **Sensor B**: Números del 15 al 35.

Debes crear RDDs a partir de las listas de números de cada sensor. Una vez hecho esto, para el **Sensor A**, transforma cada número en una tupla `(número, número al cubo)`. Y filtra solo aquellos números cuyo cubo sea **múltiplo de 7** y **mayor que 50**. El RDD resultante se almacenará en una variable llamada `rdd_a_filtrado`. Finalmente, agrupa los números filtrados según si son **pares, impares o múltiplos de 5** (un número puede pertenecer a más de un grupo), y guarda este resultado en `rdd_a_grupos`.

Volviendo a los RDDs iniciales, calcula la intersección entre los RDDs de **Sensor A y Sensor B**, guárdalo en `rdd_interseccion` y calcula la diferencia de **Sensor B menos Sensor A**, que se guardará en `rdd_diferencia`. A continuación, realiza una unión de ambos RDDs, eliminando los valores duplicados y ordénala de mayor a menor, guardando el resultado en `rdd_union`.

- Imprime los resultados de cada una de las operaciones realizadas utilizando el método `collect()`.

```python
# 2.1. Crear listas base (datos de sensores)
sensor_a = list(range(1, 26))   # Números del 1 al 25
sensor_b = list(range(15, 36))  # Números del 15 al 35

# Crear RDDs a partir de las listas
rdd_a = sc.parallelize(sensor_a)
rdd_b = sc.parallelize(sensor_b)
```
- Se crean RDDs distribuidos `rdd_a` y `rdd_b` para procesamiento paralelo.

```python
# 2.2. Transformar RDD A en tuplas (n, n^3)
rdd_a_tuplas = rdd_a.map(lambda x: (x, x**3))
```
- Cada elemento de `rdd_a` se convierte en `(número, número^3)`.

```python
# 2.3. Filtrar aquellos cuyo cubo sea múltiplo de 7 y mayor que 50
rdd_a_filtrado = rdd_a_tuplas.filter(lambda x: x[1] % 7 == 0 and x[1] > 50)

print("\nRDD A Filtrado (número, cubo):")
print(rdd_a_filtrado.collect())
```
- Se eliminan números cuyo cubo no cumple ambas condiciones.
  
> RDD A Filtrado (número, cubo):
> 
> [Stage 0:>                                                        (0 + 28) / 28]
> 
> [(7, 343), (14, 2744), (21, 9261)]


```python
# 2.4. Agrupar los números filtrados por tipo: par, impar, múltiplo de 5
def clasificar_numero(n):
    grupos = []
    if n % 2 == 0:
        grupos.append(("par", n))
    if n % 2 != 0:
        grupos.append(("impar", n))
    if n % 5 == 0:
        grupos.append(("multiplo_5", n))
    return grupos

rdd_a_grupos = rdd_a_filtrado.flatMap(lambda x: clasificar_numero(x[0])) \
                             .groupByKey() \
                             .mapValues(list)

print("\nRDD A Grupos:")
print(rdd_a_grupos.collect())
```
- Nota: un número puede pertenecer a más de un grupo (no hay aquí, pero la función permite).

> RDD A Grupos:
> [('par', [14]), ('impar', [7, 21])]


```python
# 2.5. Intersección entre Sensor A y Sensor B
rdd_interseccion = rdd_a.intersection(rdd_b)
print("\nRDD Intersección:")
print(rdd_interseccion.collect())
```
- Números presentes en ambos sensores:
> RDD Intersección:
> [15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]


```python
# 2.6. Diferencia (Sensor B - Sensor A)
rdd_diferencia = rdd_b.subtract(rdd_a)
print("\nRDD Diferencia (B - A):")
print(rdd_diferencia.collect())
```
- Números que están en B pero no en A:
> RDD Diferencia (B - A):
> [26, 27, 28, 29, 30, 31, 32, 33, 34, 35]

```python
# 2.7. Unión sin duplicados y ordenada de mayor a menor
rdd_union = rdd_a.union(rdd_b).distinct().sortBy(lambda x: -x)
print("\nRDD Unión (sin duplicados, ordenada desc):")
print(rdd_union.collect())
```
- Todos los números de A y B, sin repetidos, de mayor a menor:
> RDD Unión (sin duplicados, ordenada desc):
> [35, 34, 33, 32, 31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]

### **Ejercicio 3**: Análisis de Datos de Tweets en PySpark (*1.25 puntos*)

En este ejercicio, trabajarás con un archivo JSON llamado `tweets_sample.json` que se encuentra en la ruta `/aula_M2.858/data/tweets_sample.json`. Este archivo contiene datos de tweets y métricas relacionadas. Deberás utilizar PySpark para realizar un análisis de los datos. La estructura del archivo JSON incluye información como el número de retweets, likes, seguidores, y más. Sin embargo, para este ejercicio, te enfocarás en procesar y analizar el contenido textual de los tweets.

- Carga el archivo JSON en un RDD utilizando el método `textFile()`. Examina la estructura de los datos para identificar cómo extraer el contenido relevante.

- Extrae el campo tweets de cada uno de los tweets. Define y aplica una función para limpiar el texto. Esta función debe eliminar la puntuación, convertir el texto a minúsculas y asegurar que haya un solo espacio entre las palabras.

- Divide el texto en palabras y filtra las palabras para quedarte con aquellas que tengan menos de 7 caracteres. Después, realiza un conteo de palabras distintas, guárdalo en la variable `palabras_distintas_rdd`.

- Por último, encuentra las 5 palabras más frecuentes que terminen en vocal. Guárdalo en la variable `top_5_palabras`.

- Imprime los resultados de cada una de las operaciones realizadas.

```python
# 3.1. Cargar el archivo JSON como RDD
tweets_rdd = sc.textFile("/aula_M2.858/data/tweets_sample.json")

# Verificar que se ha cargado correctamente (mostrar 4 líneas)
print("\nVerificación de carga (4 primeras líneas):")
for linea in tweets_rdd.take(4):
    print(linea)
```


> Verificación de carga (4 primeras líneas):
> {"tweet_id": 1, "user": "usuario1", "followers": 150, "retweets": 5, "likes": 10, "tweet": "¡Hola mundo! Este es un tweet de prueba para ver cómo funciona. #prueba #mundo"}
> {"tweet_id": 2, "user": "usuario2", "followers": 300, "retweets": 2, "likes": 7, "tweet": "Los datos son el nuevo petróleo. Analiza, visualiza y actúa. #data #análisis"}
> {"tweet_id": 3, "user": "usuario3", "followers": 500, "retweets": 15, "likes": 20, "tweet": "Un día productivo en la oficina. ¿Alguna vez has tenido un día así? #productividad"}
> {"tweet_id": 4, "user": "usuario4", "followers": 250, "retweets": 10, "likes": 5, "tweet": "¿Sabías que Python es uno de los lenguajes de programación más utilizados? #Python #programación"}


```python
# 3.2. Extraer el campo 'tweet' 
import json # Necesario para trabajar con ficheros json
def extraer_texto(linea):
    try:
        data = json.loads(linea)
        return data.get("tweet", "")
    except:
        return ""

tweets_texto_rdd = tweets_rdd.map(extraer_texto).filter(lambda x: x != "")

# Verificación: mostrar 5 tweets procesados correctamente
print("\nVerificación de extracción (5 tweets):")
for t in tweets_texto_rdd.take(5):
    print("-", t)
```

> Verificación de extracción (5 tweets):
> - ¡Hola mundo! Este es un tweet de prueba para ver cómo funciona. #prueba #mundo
> - Los datos son el nuevo petróleo. Analiza, visualiza y actúa. #data #análisis
> - Un día productivo en la oficina. ¿Alguna vez has tenido un día así? #productividad
> - ¿Sabías que Python es uno de los lenguajes de programación más utilizados? #Python #programación
> - La programación puede ser divertida y emocionante. ¡No te rindas! #programación


```python
# 3.3. Limpiar el texto: minúsculas, sin puntuación, espacios simples
import re # Necesaria para procesar texto mediante patrones

def limpiar_texto(t):
    t = t.lower()
    t = re.sub(r"[^a-záéíóúüñ0-9\s]", " ", t)  # reemplaza todo lo que no sea letra, número o espacio por un espacio
    t = re.sub(r"\s+", " ", t).strip()         # colapsa varios espacios consecutivos en uno solo
    return t

tweets_limpios_rdd = tweets_texto_rdd.map(limpiar_texto)

# Verificación: mostrar 5 tweets limpios
print("\nVerificación de limpieza (5 tweets):")
for t in tweets_limpios_rdd.take(5):
    print("-", t)
```


> Verificación (5 tweets limpios y normalizados):
> - hola mundo este es un tweet de prueba para ver como funciona prueba mundo
> - los datos son el nuevo petroleo analiza visualiza y actua data analisis
> - un dia productivo en la oficina alguna vez has tenido un dia asi productividad
> - sabias que python es uno de los lenguajes de programacion mas utilizados python programacion
> - la programacion puede ser divertida y emocionante no te rindas programacion

```python
# 3.4. Dividir en palabras y filtrar las de menos de 7 caracteres
palabras_rdd = tweets_limpios_rdd.flatMap(lambda t: t.split(" "))
palabras_filtradas_rdd = palabras_rdd.filter(lambda p: len(p) < 7 and p != "")

# Verificación: mostrar 20 palabras para comprobar
print("\nVerificación de división y filtrado (20 palabras):")
print(palabras_filtradas_rdd.take(20))

```

> Verificación de división y filtrado (20 palabras):
> ['hola', 'mundo', 'este', 'es', 'un', 'tweet', 'de', 'prueba', 'para', 'ver', 'como', 'prueba', 'mundo', 'los', 'datos', 'son', 'el', 'nuevo', 'y', 'actua']
> 

```python
# 3.5. Contar palabras distintas
palabras_distintas_rdd = palabras_filtradas_rdd.distinct()
print("\nPalabras distintas (muestra):")
print(palabras_distintas_rdd.take(10))
```

> Palabras distintas (muestra):
> ['hola', 'mundo', 'este', 'prueba', 'para', 'como', 'los', 'datos', 'son', 'el']

```python
# 3.6. Encontrar las 5 palabras más frecuentes que terminen en vocal
def termina_en_vocal(p):
    return len(p) > 0 and p[-1] in "aeiouáéíóú"

palabras_vocal_rdd = palabras_filtradas_rdd.filter(termina_en_vocal)
top_5_palabras = (palabras_vocal_rdd
                  .map(lambda p: (p, 1))
                  .reduceByKey(lambda a, b: a + b)
                  .sortBy(lambda x: -x[1])
                  .take(5))

print("\nTop 5 palabras más frecuentes que terminan en vocal:")
print(top_5_palabras)
```

> Top 5 palabras más frecuentes que terminan en vocal:
> [('de', 5), ('la', 4), ('para', 3), ('mundo', 2), ('prueba', 2)]