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

### **Ejercicio 4**: Optimización de Cálculos con Persistencia (*0.25 puntos*)

Para reducir los tiempos de ejecución en Spark, es fundamental utilizar la persistencia de un RDD mediante el método `persist()`. Esta técnica es particularmente útil cuando se realizan múltiples operaciones repetidas sobre un mismo RDD.

Cuando persistes un RDD, Spark almacena los datos en memoria (o en disco, dependiendo del nivel de persistencia, para ver mas sobre los niveles de persistencia ir a la web [Persistencia Spark](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)) para evitar recomputaciones cada vez que se necesita realizar una acción sobre el RDD. Esto significa que cada nodo del clúster guarda en su memoria las particiones del RDD que ha procesado, permitiendo que las siguientes operaciones sobre el RDD sean mucho más rápidas.

**Medición de Rendimiento**

Para medir la mejora en los tiempos de ejecución, podemos utilizar la función mágica `%%time` en un entorno Jupyter Notebook, que permite observar:

- Wall clock time: Tiempo total real que lleva ejecutar una tarea, incluyendo la CPU, el tiempo de entrada/salida (I/O), y las posibles comunicaciones entre nodos en el clúster.
- CPU time: Tiempo efectivo en que la CPU está ocupada ejecutando la tarea, excluyendo otras latencias como la de entrada/salida.

En este ejercicio, se explorará el uso de la persistencia en RDDs (Resilient Distributed Datasets) utilizando PySpark. El objetivo es observar cómo la persistencia afecta al rendimiento de las operaciones de transformación y acción sobre los RDDs.

- Crea un RDD a partir de una lista de números que va del 1 al 10.000.
- Filtra el RDD para obtener solo los números mayores a 5.000 y almacena este resultado en un nuevo RDD.
- Aplica una transformación para duplicar los valores del RDD filtrado y guárdalo en un nuevo RDD.

```python
# 4.1. Crear el RDD con numeros del 1 al 10 000
rdd_numeros = sc.parallelize(range(1, 10001))

# 4.2. Filtrar los números mayores a 5000
rdd_filtrado = rdd_numeros.filter(lambda x: x > 5000)

# 4.3. Transformar (duplicar los valores)
rdd_duplicado = rdd_filtrado.map(lambda x: (x, x * 2))
```

- Utiliza el método collect() para recuperar y mostrar los números mayores a 5.000 y sus dobles, y mide el tiempo que tarda en ejecutarse esta operación utilizando la función mágica `%%time`.

```python
%%time
# 4.4. Ejecutar sin persistencia
resultado_sin_persistencia = rdd_duplicado.collect()

# Mostrar los primeros 10 resultados
print("Sin persistencia (10 primeros registros):", resultado_sin_persistencia[:10])
```

> Sin persistencia (10 primeros registros): [(5001, 10002), (5002, 10004), (5003, 10006), (5004, 10008), (5005, 10010), (5006, 10012), (5007, 10014), (5008, 10016), (5009, 10018), (5010, 10020)]
> CPU times: user 9.43 ms, sys: 2.94 ms, total: 12.4 ms
> Wall time: 149 ms


- Aplica la persistencia sobre el RDD de números mayores a 5.000 para que su contenido se mantenga en memoria entre las operaciones.

```python
from pyspark import StorageLevel

# 4.5.Aplicar persistencia
rdd_filtrado.persist(StorageLevel.MEMORY_ONLY) # Opcion más eficiente para la CPU
```

> PythonRDD[60] at RDD at PythonRDD.scala:53

- Vuelve a ejecutar el método collect() como antes. Compara este tiempo con el tiempo de la primera ejecución. (Puedes ejecutarlo varias veces y ver que ocurre con el tiempo de procesamiento.)

```python
# 4.6. Recalculo la transformacion sobre el RDD persistido
rdd_doblado_persistido = rdd_filtrado.map(lambda x: (x, x * 2))

%%time
resultado_con_persistencia = rdd_doblado_persistido.collect()

# Mostrar los primeros 10 resultados
print("Con persistencia (10 primeros registros)",  resultado_con_persistencia[:10]) ## Con cada ejecución el Wall time disminuye hasta llegar a un umbral en el que varia muy poco hacia arriba y hacia abajo
```

> Con persistencia (10 primeros registros) [(5001, 10002), (5002, 10004), (5003, 10006), (5004, 10008), (5005, 10010), (5006, 10012), (5007, 10014), (5008, 10016), (5009, 10018), (5010, 10020)]
> CPU times: user 7.47 ms, sys: 7.07 ms, total: 14.5 ms
> Wall time: 120 ms

- Deshazte de la persistencia del RDD utilizando unpersist() para liberar recursos y detén la sesión de Spark al final del ejercicio con sc.stop().
```python
# 4.7. Libero la memoria
rdd_filtrado.unpersist()

# Detener spark
sc.stop()
```

Al terminar el ejercicio, analiza y comenta los resultados obtenidos, explicando cómo la persistencia afectó el rendimiento de tus cálculos.

> ## Analisis y comentario de los resultados obtenidos:
> #> Tras aplicar persistencia, el tiempo total de ejecución (Wall time) se ha reducido. 
> #> Esto se debe a que Spark evita recalcular el RDD filtrado en memoria y reutiliza los datos cacheados. 
> #> Además, en ejecuciones repetidas el tiempo mejora aún más, evidenciando el beneficio de la persistencia.

# **Apache Spark Dataframes**

En esta parte de la práctica vamos a introducir los elementos que ofrece Spark para trabajar con estructuras de datos. Veremos desde estructuras muy simples hasta estructuras complejas, donde los campos pueden a su vez tener campos anidados. En concreto utilizaremos datos de Twitter capturados en el contexto de las elecciones generales en España del 28 de abril de 2019

### Configuración del entorno

```python
import findspark
import os

SPARK_HOME_PATH = "/usr/bigtop/current/spark-client/" 
os.environ['SPARK_HOME'] = SPARK_HOME_PATH
findspark.init(SPARK_HOME_PATH)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ActividadSparkSQL_usuario") \
    .master("local[1]") \
    .enableHiveSupport() \
    .getOrCreate()
#    .config("spark.hadoop.hive.execution.engine", "mr") \
#    .enableHiveSupport() \

print(spark.sparkContext.appName)
print(spark.version)

sc=spark.sparkContext
```
```python
import re
import os
import pandas as pd
from matplotlib import pyplot as plt
from math import floor
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import Row
```
```python
SUBMIT_ARGS = "--jars /opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/jars/graphframes_graphframes-0.7.0-spark2.4-s_2.11.jar pyspark-shell"

os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
```

## Introducción a dataframes estructurados y operaciones sobre ellos

Como ya se ha mencionado, en los siguientes ejercicis vamos a utilizar datos de Twitter que recolectamos durante las elecciones generales en España del 28 de abril de 2019. Como veremos, los tweets tienen una estructura interna bastante compleja que hemos simplificado un poco en esta práctica.

Lo primero que vamos a aprender es cómo importar este tipo de datos a nuestro entorno. Uno de los tipos de archivos más comunes para guardar este formato de información es [la estructura JSON](https://en.wikipedia.org/wiki/JSON). Esta estructura permite guardar información en un texto plano de diferentes objetos siguiendo una estructura de diccionario donde cada campo tiene asignado una clave y un valor. La estructura puede ser anidada, o sea que una clave puede tener como valor otra estructura de tipo diccionario.

Spark SQL permite leer datos de muchos formatos diferentes. Se os pide que [leáis el fichero JSON](https://spark.apache.org/docs/2.4.0/sql-data-sources-json.html) de la ruta ```/aula_M2.858/data/tweets28a_sample.json```. Este archivo contiene un pequeño *sample*, un 0.1% de la base de datos completa (en un siguiente apartado veremos cómo realizar este *sampleado*). En esta ocasión no se os pide especificar la estructura del dataframe ya que la función de lectura la inferirá automáticamente.

**Ejemplo de lectura (Rellenar con lo correspondiente para la lectura del archivo json)**:

```Python
tweets_sample = spark.read.json(<FILL IN>)

print("Loaded dataset contains %d tweets" % tweets_sample.count())
```

Para mostrar la estructura del dataset que acabamos de cargar, podéis obtener la información acerca de cómo está estructurado el DataTable utilizando el método ```printSchema()```. Tenéis que familiarizaros con esta estructura ya que será la que utilizaremos durante los próximos ejercicios. Recordad también que no todos los tweets tienen todos los campos, como por ejemplo la ubicación (campo ```place```). Cuando esto pasa el campo pasa a ser ```NULL```. Podéis ver más información sobre este tipo de datos en [este enlace](https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object).

Ahora debéis introducir el ejemplo de lectura con el `<FILL IN>` relleno según corresponda para la lectura del archivo JSON. Y, a continuación, mostraréis la estructura del dataset utilizando `printSchema()`.

```python
# Cargar el dataset JSON 
tweets_sample = spark.read.json("/aula_M2.858/data/tweets28a_sample.json")

# Mostrar número total de tweets cargados
print(f"El dataset cargado contiene {tweets_sample.count()} tweets.\n") 

# Mostrar la estructura del dataset
tweets_sample.printSchema()
```

> El dataset cargado contiene 27268 tweets.
> 
> root
>  |-- _id: string (nullable = true)
>  |-- created_at: long (nullable = true)
>  |-- lang: string (nullable = true)
>  |-- place: struct (nullable = true)
>  |    |-- bounding_box: struct (nullable = true)
>  |    |    |-- coordinates: array (nullable = true)
>  |    |    |    |-- element: array (containsNull = true)
>  |    |    |    |    |-- element: array (containsNull = true)
>  |    |    |    |    |    |-- element: double (containsNull = true)
>  |    |    |-- type: string (nullable = true)
>  |    |-- country_code: string (nullable = true)
>  |    |-- id: string (nullable = true)
>  |    |-- name: string (nullable = true)
>  |    |-- place_type: string (nullable = true)
>  |-- retweeted_status: struct (nullable = true)
>  |    |-- _id: string (nullable = true)
>  |    |-- user: struct (nullable = true)
>  |    |    |-- followers_count: long (nullable = true)
>  |    |    |-- friends_count: long (nullable = true)
>  |    |    |-- id_str: string (nullable = true)
>  |    |    |-- lang: string (nullable = true)
>  |    |    |-- screen_name: string (nullable = true)
>  |    |    |-- statuses_count: long (nullable = true)
>  |-- text: string (nullable = true)
>  |-- user: struct (nullable = true)
>  |    |-- followers_count: long (nullable = true)
>  |    |-- friends_count: long (nullable = true)
>  |    |-- id_str: string (nullable = true)
>  |    |-- lang: string (nullable = true)
>  |    |-- screen_name: string (nullable = true)
>  |    |-- statuses_count: long (nullable = true)
### Queries sobre dataframes complejos

A continuación vamos a ver como realizar consultas sobre el dataset de los tweets. Vamos a utilizar [sentencias *SQL*](https://www.w3schools.com/sql/default.asp) (como las utilizadas en la mayoría de bases de datos relacionales).

Lo primero que se debe hacer es registrar el dataframe de tweets como una tabla de SQL. Para ello utilizaremos [sqlContext.registerDataFrameAsTable()](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.SQLContext.registerDataFrameAsTable). Para ejecutar comandos sql solo teneis que utilizar el metodo sql() del objecto contexto, en este caso `sqlContext`.

#### Queries a través del pipeline

Las tablas de Spark SQL ofrecen otro mecanismo para aplicar las transformaciones y obtener resultados similares a los que se obtendría aplicando una consulta SQL. Por ejemplo, utilizando el siguiente pipeline obtendremos el texto de todos los tweets en español:

```
tweets_sample.where("lang == 'es'").select("text")
```

Que es equivalente a la siguiente sentencia SQL:

```
SELECT text
FROM tweets_sample
WHERE lang == 'es'
```

Podéis consultar el [API de spark SQL](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html) para encontrar más información sobre como utilizar las diferentes transformaciones en tablas.

### **Ejercicio 5**: Análisis de Tweets mediante DataFrames y consultas SQL (*2 puntos*)

Anteriormente ya has realizado la lectura del conjunto `tweets28a_sample.json` en formato JSON. Ahora deberás asegúrate de registrar el DataFrame como una tabla SQL llamada `tweets_sample`.

***Nota:*** Debido a que es posible que ejecutes estas líneas de codigo várias veces, vamos a tomar la precaución de ejecutar el comando sql para eliminar tablas antes de que las crees, ya que puede existir la posibilidad de que ya existan.

`spark.sql("DROP TABLE IF EXISTS tweets_sample")`

A continuación, se pide crear una tabla y registrarla con el nombre ```users_agg``` con [la información agregada](https://www.w3schools.com/sql/sql_groupby.asp) de los usuarios que tengan definido su idioma (```user.lang```) como español (```es```). En concreto se pide que la tabla contenga las siguientes columnas:
- **screen_name:** nombre del usuario
- **friends_count:** número máximo (ver nota) de personas a las que sigue
- **tweets:** número de tweets realizados
- **followers_count:** número máximo (ver nota) personas que siguen al usuario.

El orden en el cual se deben mostrar los registros es orden descendente acorde al número de tweets.

***Nota:*** Es importante que te fijes que el nombre de *friends* y *followers* puede diferir a lo largo de la adquisición de datos. En este caso vamos a utilizar la función de agregación `MAX` sobre cada uno de estos campos para evitar segmentar el usuario en diversas instancias.


```python
# Eliminar tabla si ya existe
spark.sql("DROP TABLE IF EXISTS tweets_sample")

# Registrar el DataFrame como tabla SQL
tweets_sample.createOrReplaceTempView("tweets_sample")

# Crear tabla agregada con usuarios en español
users_agg = spark.sql("""
    SELECT
        user.screen_name AS screen_name,
        MAX(user.friends_count) AS friends_count,
        COUNT(*) AS tweets,
        MAX(user.followers_count) AS followers_count
    FROM tweets_sample
    WHERE user.lang = 'es'
    GROUP BY user.screen_name
    ORDER BY tweets DESC
""")

# Mostrar algunos resultados
users_agg.show(10)
```

> [Stage 36:=================================================>        (6 + 1) / 7]
> 
> +---------------+-------------+------+---------------+
> |    screen_name|friends_count|tweets|followers_count|
> +---------------+-------------+------+---------------+
> |       anaoromi|         6258|    16|           6774|
> |    RosaMar6254|         6208|    14|           6245|
> |        lyuva26|         3088|    13|           3732|
> |PisandoFuerte10|         2795|    12|           1752|
> |     carrasquem|          147|    12|            215|
> |       jasalo54|         1889|    11|            689|
> |     Lordcrow11|         5002|     9|           3069|
> |      lolalailo|         4922|     9|           3738|
> |  PabloChabolas|         4925|     9|           4042|
> |      kikyosanz|          154|     9|            273|
> +---------------+-------------+------+---------------+
> only showing top 10 rows

A continuación recurriremos al [JOIN de tablas](https://www.w3schools.com/sql/sql_join.asp) para combinar la información entre tablas. Debes combinar la tabla `users_agg` y la tabla `tweets_sample` utilizando un `INNER JOIN` para obtener una nueva tabla con el nombre retweeted con la siguiente información:

- _**screen_name:**_ nombre de usuario
- _**friends_count:**_ número máximo de personas a las que sigue
- _**followers_count:**_ número máximo de personas que siguen al usuario.
- _**tweets:**_ número de tweets realizados por el usuario.
- _**retweeted:**_ número de retweets obtenidos por el usuario.
- _**ratio_tweet_retweeted:**_ ratio de retweets por número de tweets publicados 

La tabla resultante tiene que estar ordenada de manera descendente según el valor de la columna `ratio_tweet_retweeted`.

Por último, utilizando queries a través de pipeline, debes crear una tabla `user_retweets` a partir de la tabla `tweets_sample`, utilizando transformaciones que contenga dos columnas:

- _**screen_name:**_ nombre de usuario
- _**retweeted:**_ número de retweets

Ordena la tabla en orden descendente utilizando el valor de la columna `retweeted`.

```python
# 5.2.1.
# Eliminar tabla si ya existe
spark.sql("DROP TABLE IF EXISTS users_agg")

# Registrar users_agg como tabla SQL
users_agg.createOrReplaceTempView("users_agg")

# Crear tabla retweeted mediante INNER JOIN
retweeted = spark.sql("""
    SELECT
        ua.screen_name,
        ua.friends_count,
        ua.followers_count,
        ua.tweets,
        COUNT(*) AS retweeted,
        COUNT(*) / ua.tweets AS ratio_tweet_retweeted
    FROM users_agg ua
        INNER JOIN tweets_sample ts ON ua.screen_name = ts.retweeted_status.user.screen_name
    GROUP BY 
        ua.screen_name, 
        ua.friends_count, 
        ua.followers_count, 
        ua.tweets
    ORDER BY ratio_tweet_retweeted DESC
""")

# Mostrar información de la tabla
print(f"La tabla retweeted contiene {retweeted.count()} registros.\n")
print(f"Con las siguientes columnas: {retweeted.columns}.\n")
retweeted.printSchema()

# Mostrar los primeros resultados
print("\nPrimeras filas de la tabla retweeted:")
retweeted.show(10)
```

> Primeras filas de la tabla retweeted:
> 
> +--------------+-------------+---------------+------+---------+---------------------+
> |   screen_name|friends_count|followers_count|tweets|retweeted|ratio_tweet_retweeted|
> +--------------+-------------+---------------+------+---------+---------------------+
> |          PSOE|        13635|         671073|     1|      155|                155.0|
> |  CiudadanosCs|        92910|         511896|     1|      117|                117.0|
> |     JuntsXCat|          202|          88515|     1|       73|                 73.0|
> |  PartidoPACMA|         1498|         232932|     1|       63|                 63.0|
> |  pablocasado_|         4567|         238926|     1|       50|                 50.0|
> |voxnoticias_es|         2146|          29582|     1|       44|                 44.0|
> |RaiLopezCalvet|         7579|          13574|     1|       43|                 43.0|
> |        iunida|        10225|         558318|     1|       39|                 39.0|
> |        Xuxipc|          311|         184967|     1|       37|                 37.0|
> |       Panik81|         1587|          15374|     1|       29|                 29.0|
> +--------------+-------------+---------------+------+---------+---------------------+
> only showing top 10 rows


```python
# 5.2.2.
# Crear tabla user_retweets mediante transformaciones con pipeline
user_retweets = (tweets_sample
    .filter(tweets_sample.retweeted_status.isNotNull())  # Filtrar solo tweets con retweets
    .select("retweeted_status.user.screen_name")         # Seleccionar el screen_name
    .groupBy("screen_name")                              # Agrupar por usuario
    .count()                                             # Contar retweets
    .withColumnRenamed("count", "retweeted")             # Renombrar columna
    .orderBy("retweeted", ascending=False)               # Ordenar descendente
)

# Mostrar información de la tabla
print(f"La tabla user_retweets contiene {user_retweets.count()} registros.\n")
print(f"Con las siguientes columnas: {user_retweets.columns}.\n")
user_retweets.printSchema()

# Mostrar los primeros resultados
print("Primeras filas de la tabla user_retweets:")
user_retweets.show(10)
```

> La tabla user_retweets contiene 7664 registros.
> 
> Con las siguientes columnas: ['screen_name', 'retweeted'].
> 
> root
>  |-- screen_name: string (nullable = true)
>  |-- retweeted: long (nullable = false)
> 
> Primeras filas de la tabla user_retweets:
> 
> [Stage 42:=========================================>                (5 + 1) [/](https://eimtcld3.uoclabs.uoc.es/) 7]
> 
> +--------------+---------+
> |   screen_name|retweeted|
> +--------------+---------+
> |        vox_es|      299|
> | Santi_ABASCAL|      238|
> |  ahorapodemos|      238|
> |      iescolar|      166|
> | AlbanoDante76|      161|
> |          PSOE|      155|
> |AntonioMaestre|      154|
> |          KRLS|      149|
> |        boye_g|      142|
> |  CiudadanosCs|      117|
> +--------------+---------+
> only showing top 10 rows

## Bases de datos HIVE y operaciones complejas

Hasta ahora hemos estado trabajando con un pequeño sample de los tweets generados (el 0.1%). En esta parte de la actividad vamos a ver como trabajar y tratar con el dataset completo. Para ello vamos a utilizar tanto transformaciones sobre tablas como operaciones sobre RDD cuando sea necesario.

Es importante tener en cuenta que muchas veces los datos con los que trabajamos se utilizarán en diversos proyectos. En lugar de manejar directamente los archivos, es más eficiente y organizado recurrir a una base de datos para gestionar la información. En el ecosistema de Hadoop, una de las bases de datos más utilizadas es [Apache Hive](https://hive.apache.org/). Sin embargo, es crucial entender que Hive no es una base de datos convencional. En realidad, funciona como un metastore que mapea archivos en el sistema de archivos distribuido de Hadoop (HDFS).

Esto significa que Hive no almacena los datos en su propio formato de base de datos, sino que actúa como una interfaz que permite a los usuarios ejecutar consultas SQL sobre los datos almacenados en HDFS. Esto proporciona una forma eficiente de acceder y manipular grandes volúmenes de datos distribuidos sin necesidad de moverlos o convertirlos a un formato tradicional de base de datos.

La manera de acceder a esta base de datos es tal y como se muestra en el siguiente código (debéis ejecutarlo).

```python
# 1. Obtener la lista de objetos 'Table'
tables_list = spark.catalog.listTables()

# 2. Convertir la lista de objetos en una lista de tuplas (Nombre, BD, Temporal)
# Utilizamos una comprensión de lista (list comprehension)
data_for_df = [(t.name, t.database, t.isTemporary) for t in tables_list]

# 3. Definir el esquema manualmente para evitar cualquier inferencia automática
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

manual_schema = StructType([
    StructField("Nombre_Tabla", StringType(), True),
    StructField("Base_Datos", StringType(), True),
    StructField("Es_Temporal", BooleanType(), True)
])

# 4. Crear el DataFrame con los datos y el esquema definido
tables_df = spark.createDataFrame(data_for_df, schema=manual_schema)

# 5. Mostrar el resultado
tables_df.show()
```

> +-----------------+----------+-----------+
> |     Nombre_Tabla|Base_Datos|Es_Temporal|
> +-----------------+----------+-----------+
> |        boxscores|   default|      false|
> |    boxscores_ext|   default|      false|
> |boxscores_interna|   default|      false|
> |boxscores_managed|   default|      false|
> |    boxscores_orc|   default|      false|
> |   boxscores_parq|   default|      false|
> |    ext_boxscores|   default|      false|
> |        ext_games|   default|      false|
> |     ext_injuries|   default|      false|
> |      ext_players|   default|      false|
> |        ext_teams|   default|      false|
> |            games|   default|      false|
> |        games_ext|   default|      false|
> |    games_interna|   default|      false|
> |    games_managed|   default|      false|
> |        games_orc|   default|      false|
> |       games_parq|   default|      false|
> |         injuries|   default|      false|
> |     injuries_ext|   default|      false|
> | injuries_interna|   default|      false|
> +-----------------+----------+-----------+
> only showing top 20 rows

### Más allá de las transformaciones SQL

Algunas veces vamos a necesitar obtener resultados que precisan operaciones que van más allá de lo que podemos conseguir (cómodamente) utilizando el lenguaje SQL. En esta parte de la práctica vamos a practicar cómo pasar de una tabla a un RDD, para hacer operaciones complejas, y luego volver a pasar a una tabla.

Ahora viene la parte interesante. Una tabla puede convertirse en un RDD a través del atributo ```.rdd```. Este atributo guarda la información de la tabla en una lista donde cada elemento es un [objeto del tipo ```Row```](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Row). Los objetos pertenecientes a esta clase pueden verse como diccionarios donde la información de las diferentes columnas queda reflejada en forma de atributo. Por ejemplo, imaginad que tenemos una tabla con dos columnas, nombre y apellido, si utilizamos el atributo ```.rdd``` de dicha tabla obtendremos una lista con objetos del tipo row donde cada objeto tiene dos atributos: nombre y apellido. Para acceder a los atributos solo tenéis que utilizar la sintaxis *punto* de Python, e.g., ```row.nombre``` o ```row.apellido```.

### **Ejercicio 6**: Análisis de Tweets Geolocalizados (*1.5 puntos*)

Dada la tabla de tweets `tweets28a_sample25`, debes crear una variable `tweets` utilizando el objeto `spark` y el método `table()`. Utilizando una sentencia SQL, se requiere extraer información sobre los tweets que contienen datos geolocalizados (es decir, aquellos donde el campo `place` no es nulo) y determinar cuántos tweets se han generado desde cada lugar. Los resultados deben ser presentados en orden descendente por la cantidad de tweets.

**Esquema sentencia sql**
```Python
tweets_place = spark.sql(<FILL IN>)
```

A continuación, crea una tabla llamada `tweets_place` que contenga dos columnas:

- ***name:*** nombre del lugar desde donde se ha generado el tweet.
- ***tweets:*** número total de tweets realizados en dicho lugar.

Finalmente, muestra los 10 lugares con mayor número de tweets en la tabla resultante.

Adicionalmente, crea una tabla llamada `tweets_geo` que contenga únicamente los tweets que tienen información de geolocalización, y asegúrate de que incluya el nombre del lugar. A partir de esta tabla, crea un objeto ```tweets_place_rdd``` que contenga una lista de tuplas con la información ```(name, tweets)``` sobre el nombre del lugar y el número de tweets generados desde allí.

Una vez generado este RDD vamos a crear una tabla. El primer paso es generar por cada tupla un objeto Row que contenga un atributo ```name``` y un atributo ```tweets```. Ahora solo tenéis que aplicar el método ```toDF()``` para generar una tabla. Ordenad las filas de esta tabla por el número de tweets en orden descendente.

El ejercicio deberá realizarse combinando tanto sentencias SQL como RDD en Apache Spark.

```python
# 6.1.
# Cargar la tabla tweets28a_sample25 en una variable
tweets = spark.table("tweets28a_sample25")

# Mostrar información de la tabla
print(f"La tabla tweets contiene {tweets.count()} tweets.\n")
print(f"Con las siguientes columnas: {tweets.columns}.\n")
tweets.printSchema()

# Mostrar los primeros resultados
print("Primeras filas de la tabla tweets:")
tweets.show(10)
```

> La tabla tweets contiene 6354961 tweets.
> 
> Con las siguientes columnas: ['_id', 'created_at', 'lang', 'place', 'retweeted_status', 'text', 'user'].
> 
> root
>  |-- _id: string (nullable = true)
>  |-- created_at: timestamp (nullable = true)
>  |-- lang: string (nullable = true)
>  |-- place: struct (nullable = true)
>  |    |-- bounding_box: struct (nullable = true)
>  |    |    |-- coordinates: array (nullable = true)
>  |    |    |    |-- element: array (containsNull = true)
>  |    |    |    |    |-- element: array (containsNull = true)
>  |    |    |    |    |    |-- element: double (containsNull = true)
>  |    |    |-- type: string (nullable = true)
>  |    |-- country_code: string (nullable = true)
>  |    |-- id: string (nullable = true)
>  |    |-- name: string (nullable = true)
>  |    |-- place_type: string (nullable = true)
>  |-- retweeted_status: struct (nullable = true)
>  |    |-- _id: string (nullable = true)
>  |    |-- user: struct (nullable = true)
>  |    |    |-- followers_count: long (nullable = true)
>  |    |    |-- friends_count: long (nullable = true)
>  |    |    |-- id_str: string (nullable = true)
>  |    |    |-- lang: string (nullable = true)
>  |    |    |-- screen_name: string (nullable = true)
>  |    |    |-- statuses_count: long (nullable = true)
>  |-- text: string (nullable = true)
>  |-- user: struct (nullable = true)
>  |    |-- followers_count: long (nullable = true)
>  |    |-- friends_count: long (nullable = true)
>  |    |-- id_str: string (nullable = true)
>  |    |-- lang: string (nullable = true)
>  |    |-- screen_name: string (nullable = true)
>  |    |-- statuses_count: long (nullable = true)
> 
> Primeras filas de la tabla tweets:
> 
> [Stage 45:>                                                         (0 + 1) [/](https://eimtcld3.uoclabs.uoc.es/) 1]
> 
> +-------------------+-------------------+----+-----+--------------------+--------------------+--------------------+
> |                _id|         created_at|lang|place|    retweeted_status|                text|                user|
> +-------------------+-------------------+----+-----+--------------------+--------------------+--------------------+
> |1117169839657844736|2019-04-13 22:57:00|  en| null|{1117009066402955...|RT @DearAuntCrabb...|{329, 871, 547969...|
> |1117169840802934787|2019-04-13 22:57:00|  es| null|{1117037292458262...|RT @PAH_Burgos: #...|{18, 17, 88521307...|
> |1117169840870100994|2019-04-13 22:57:00|  es| null|{1116682791503192...|RT @ViajesFalcon:...|{1270, 356, 76286...|
> |1117169840886882304|2019-04-13 22:57:00|  es| null|{1117157909568397...|RT @AiniApgg: Est...|{3887, 3860, 6100...|
> |1117169841985683456|2019-04-13 22:57:00|  es| null|{1117083000343232...|RT @Santi_ABASCAL...|{137, 593, 289042...|
> |1117169842329653248|2019-04-13 22:57:01|  es| null|{1116442625911992...|RT @CarmenPicazoC...|{111, 84, 3063577...|
> |1117169844984610817|2019-04-13 22:57:01|  es| null|                null|@Earco1977 Pues a...|{5893, 4783, 5090...|
> |1117169845798305792|2019-04-13 22:57:01|  es| null|{1117108350993539...|RT @FrayJosepho: ...|{3834, 1582, 6116...|
> |1117169846037426176|2019-04-13 22:57:01|  es| null|{1116961638589059...|RT @FiloPolitics:...|{243, 136, 836025...|
> |1117169846272315392|2019-04-13 22:57:02| und| null|                null|@CastigadorY @jus...|{1900, 1898, 2573...|
> +-------------------+-------------------+----+-----+--------------------+--------------------+--------------------+
> only showing top 10 rows

```python
# 6.2.
# Consulta SQL para extraer lugares con tweets geolocalizados
tweets_place = spark.sql("""
    SELECT
        place.name as name,
        COUNT(*) AS tweets
    FROM tweets28a_sample25
    WHERE place IS NOT NULL
    GROUP BY place.name
    ORDER BY tweets DESC
""")

# Mostrar información de la tabla 
print(f"La tabla tweets_place contiene {tweets_place.count()} lugares únicos.\n")
print(f"Con las siguientes columnas: {tweets_place.columns}.\n")
tweets_place.printSchema()

# Mostrar los primeros resultados
print("Primeras filas de la tabla tweets:")
tweets_place.show(10)
```

> La tabla tweets_place contiene 4038 lugares únicos.
> 
> Con las siguientes columnas: ['name', 'tweets'].
> 
> root
>  |-- name: string (nullable = true)
>  |-- tweets: long (nullable = false)
> 
> Primeras filas de la tabla tweets:
> 
> [Stage 67:==================================================>       (7 + 1) [/](https://eimtcld3.uoclabs.uoc.es/) 8]
> 
> +-----------+------+
> |       name|tweets|
> +-----------+------+
> |     Madrid|  4911|
> |  Barcelona|  3481|
> |    Sevilla|   959|
> |   Valencia|   689|
> |   Zaragoza|   597|
> |Villamartín|   570|
> |     Málaga|   546|
> |     Murcia|   461|
> |      Palma|   416|
> |   Alicante|   407|
> +-----------+------+
> only showing top 10 rows

```python
# 6.3.
# Crear tabla con tweets que tienen geolocalizacion
tweets_geo = spark.sql("""
    SELECT *
    FROM tweets28a_sample25
    WHERE place IS NOT NULL
""")

# Mostrar información de la tabla 
print(f"\nLa tabla tweets_geo contiene {tweets_geo.count()} tweets geolocalizados.\n")
```

> La tabla tweets_geo contiene 44477 tweets geolocalizados.

```python
# 6.4.
# Convertir la tabla tweets_geo a rdd
tweets_place_rdd = (tweets_geo
    .select("place.name")                    # Selecciono el nombre del lugar
    .rdd                                     # Convierto a rdd
    .map(lambda row: (row.name, 1))          # Crear tuplas 'nombre lugar' + 1
    .reduceByKey(lambda a, b: a + b)         # Sumar conteos por lugar
    .sortBy(lambda x: x[1], ascending=False) # Ordenar descendente
)

# Mostrar los primeros resultados
print("Primeras 10 tuplas del RDD (nombre lugar, tweets):")
tweets_place_rdd.take(10)
```

> Primeras 10 tuplas del RDD (nombre lugar, tweets):
> 
> [('Madrid', 4911),
>  ('Barcelona', 3481),
>  ('Sevilla', 959),
>  ('Valencia', 689),
>  ('Zaragoza', 597),
>  ('Villamartín', 570),
>  ('Málaga', 546),
>  ('Murcia', 461),
>  ('Palma', 416),
>  ('Alicante', 407)]

```python
# 6.5.
# Crear objetos Row a partir de las tuplas
tweets_place_from_rdd = (tweets_place_rdd
    .map(lambda x: Row(name = x[0], tweets = x[1])) # Tupla a Row
    .toDF()
    .orderBy("tweets", ascending=False)
)

# Mostrar información de la tabla 
print(f"La tabla tweets_place_from_rdd contiene {tweets_place_from_rdd.count()} lugares únicos.\n")
print(f"Con las siguientes columnas: {tweets_place_from_rdd.columns}.\n")
tweets_place_from_rdd.printSchema()

# Mostrar los primeros resultados
print("Top 10 lugares con más tweets (creado desde RDD):\n")
tweets_place_from_rdd.show(10)
```

> La tabla tweets_place_from_rdd contiene 4038 lugares únicos.
> 
> Con las siguientes columnas: ['name', 'tweets'].
> 
> root
>  |-- name: string (nullable = true)
>  |-- tweets: long (nullable = true)
> 
> Top 10 lugares con más tweets (creado desde RDD):
> 
> +-----------+------+
> |       name|tweets|
> +-----------+------+
> |     Madrid|  4911|
> |  Barcelona|  3481|
> |    Sevilla|   959|
> |   Valencia|   689|
> |   Zaragoza|   597|
> |Villamartín|   570|
> |     Málaga|   546|
> |     Murcia|   461|
> |      Palma|   416|
> |   Alicante|   407|
> +-----------+------+
> only showing top 10 rows


## Sampling

En muchas ocasiones, antes de lanzar costosos procesos, es una práctica habitual tratar con un pequeño conjunto de los datos para investigar algunas propiedades o simplemente para depurar nuestros algoritmos, a esta tarea se la llama sampling. En esta parte de la práctica vamos a ver los dos principales métodos de sampling y cómo utilizarlos.

### Nota:
Para producir un gráfico de barras utilizando [Pandas](https://pandas.pydata.org/) donde se muestre la información que acabáis de generar. Primero transformad la tabla a pandas utilizando el método `toPandas()`. Plotead la tabla resultante utilizando [la funcionalidad gráfica de pandas.](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.plot.bar.html)

### Homogéneo

El primer sampling que vamos a ver es [el homogeneo](https://en.wikipedia.org/wiki/Simple_random_sample). Este sampling se basta en simplemente escoger una fracción de la población seleccionando aleatoriamente elementos de esta.

Primero de todo vamos a realizar un sampling homogéneo del 1% de los tweets generados en periodo electoral sin reemplazo. Guardad en una variable ```tweets_sample``` este sampling utilizando el método ```sample``` descrito en la [API de pyspark SQL](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html). El seed que vais a utilizar para inicializar el generador aleatorio es 42.

**Esquema**
```Python
seed = 42
fraction = 0.01

tweets_sample = tweets.<FILL IN>

print("Number of tweets sampled: {0}".format(tweets_sample.count()))
```

### **Ejercicio 7**: Análisis del Patrón de Actividad Horaria en Twitter (*1 puntos*)
A partir de una muestra del 1% de los tweets disponibles, se desea analizar el patrón de uso diario de Twitter, prestando especial atención a la actividad horaria. El objetivo es calcular y visualizar el promedio de tweets generados en cada hora del día. Para ello se debe crear una tabla ```tweets_timestamp``` con la información:
- ***created_at***: timestamp de cuando se publicó el tweet.
- ***hour***: a que hora del dia corresponde.
- ***day***: Fecha en formato MM-dd-YY

La tabla tiene que estar en orden ascendente según la columna `created_at`

**Pista**: Para crear las columnas "hour" y "day" en tu tabla tweets_timestamp, puedes utilizar withColumn(). La función ```hour``` os servirá para extraer la hora del timestamp y la función ```date_format``` os permitirá generar la fecha.

A continuación, utiliza la muestra de tweets para extraer la hora y fecha de publicación, y a partir de esa información, determina cuántos tweets se generan por hora. Asegúrate de ajustar el promedio de tweets para reflejar lo que ocurriría en el conjunto completo de datos y presenta los resultados en un gráfico de barras que muestre la actividad horaria promedio.

---
---

## Ejercicio 7: Análisis del Patrón de Actividad Horaria en Twitter

### Paso 1: Realizar *sampling* homogéneo del 1%

```python
# Configurar parámetros del sampling
seed = 42
fraction = 0.01

# Realizar sampling sin reemplazo
tweets_sample = tweets.sample(withReplacement=False, fraction=fraction, seed=seed)

print(f"Número de tweets en el sampling: {tweets_sample.count()}")
print(f"Porcentaje muestreado: {fraction * 100}%")
```


> Número de tweets en tweets_sample 64063
> Porcentaje muestreado: 1.0 %

---

### Paso 2: Crear tabla tweets_timestamp con información temporal

```python
from pyspark.sql.functions import hour, date_format

# Crear tabla con información de timestamp, hora y día
tweets_timestamp = (tweets_sample
    .withColumn("hour", hour("created_at"))                  # Extraer la hora (0-23)
    .withColumn("day", date_format("created_at", "MM-dd-yy")) # Formato fecha MM-dd-yy
    .select("created_at", "hour", "day")                     # Seleccionar solo estas columnas
    .orderBy("created_at", ascending=True)                   # Ordenar ascendentemente
)

# Mostrar información de la tabla
print(f"\nLa tabla tweets_timestamp contiene {tweets_timestamp.count()} registros.\n")
print(f"Columnas: {tweets_timestamp.columns}\n")
tweets_timestamp.printSchema()

# Mostrar primeros resultados
print("\nPrimeras 10 filas de tweets_timestamp:")
tweets_timestamp.show(10)
```

> La tabla tweets_timestamp contiene 64063 registros.
> 
> Columnas: ['created_at', 'hour', 'day']
> 
> root
>  |-- created_at: timestamp (nullable = true)
>  |-- hour: integer (nullable = true)
>  |-- day: string (nullable = true)
> 
> 
> Primeras 10 filas de tweets_timestamp:
> 
> [Stage 78:====================================>                     (5 + 1) / 8]
> 
> +-------------------+----+--------+
> |         created_at|hour|     day|
> +-------------------+----+--------+
> |2019-04-12 06:30:52|   6|04-12-19|
> |2019-04-12 08:32:05|   8|04-12-19|
> |2019-04-12 10:44:57|  10|04-12-19|
> |2019-04-12 10:45:54|  10|04-12-19|
> |2019-04-12 10:47:56|  10|04-12-19|
> |2019-04-12 10:48:14|  10|04-12-19|
> |2019-04-12 10:50:26|  10|04-12-19|
> |2019-04-12 10:50:53|  10|04-12-19|
> |2019-04-12 10:53:19|  10|04-12-19|
> |2019-04-12 10:53:23|  10|04-12-19|
> +-------------------+----+--------+
> only showing top 10 rows

---

### Paso 3: Calcular promedio de tweets por hora

```python
# Contar tweets por hora en la muestra
tweets_por_hora_muestra = (tweets_timestamp
    .groupBy("hour")                          # Agrupar por hora
    .count()                                  # Contar tweets por grupo
    .withColumnRenamed("count", "tweets")     # Renombrar columna
    .orderBy("hour")                          # Ordenar por hora
)

print("\nTweets por hora en la muestra (1%):")
tweets_por_hora_muestra.show(24)
```

> Tweets por hora en la muestra (1%):
> 
> [Stage 79:===========================================>              (6 + 1) / 8]
> 
> +----+------+
> |hour|tweets|
> +----+------+
> |   0|  4736|
> |   1|  2299|
> |   2|  1154|
> |   3|   563|
> |   4|   387|
> |   5|   354|
> |   6|   571|
> |   7|  1118|
> |   8|  1808|
> |   9|  2318|
> |  10|  2630|
> |  11|  2747|
> |  12|  2810|
> |  13|  3017|
> |  14|  3027|
> |  15|  3269|
> |  16|  3106|
> |  17|  2927|
> |  18|  3063|
> |  19|  3093|
> |  20|  3521|
> |  21|  3742|
> |  22|  5544|
> |  23|  6259|
> +----+------+

---

### Paso 4: Ajustar al conjunto completo (escalar por 100)

```python
from pyspark.sql.functions import col

# Escalar los resultados al 100% de los datos
tweets_por_hora_completo = (tweets_por_hora_muestra
    .withColumn("tweets_promedio", col("tweets") / fraction)  # Dividir por la fracción muestreada
    .select("hour", "tweets_promedio")                        # Seleccionar columnas relevantes
    .orderBy("hour")
)

print("\nPromedio de tweets por hora (ajustado al 100%):")
tweets_por_hora_completo.show(24)
```

> Promedio de tweets por hora (ajustado al 100%):
> 
> [Stage 82:===========================================>              (6 + 1) / 8]
> 
> +----+---------------+
> |hour|tweets_promedio|
> +----+---------------+
> |   0|       473600.0|
> |   1|       229900.0|
> |   2|       115400.0|
> |   3|        56300.0|
> |   4|        38700.0|
> |   5|        35400.0|
> |   6|        57100.0|
> |   7|       111800.0|
> |   8|       180800.0|
> |   9|       231800.0|
> |  10|       263000.0|
> |  11|       274700.0|
> |  12|       281000.0|
> |  13|       301700.0|
> |  14|       302700.0|
> |  15|       326900.0|
> |  16|       310600.0|
> |  17|       292700.0|
> |  18|       306300.0|
> |  19|       309300.0|
> |  20|       352100.0|
> |  21|       374200.0|
> |  22|       554400.0|
> |  23|       625900.0|
> +----+---------------+

### Paso 5: Convertir a Pandas y crear gráfico de barras

```python
import pandas as pd
import matplotlib.pyplot as plt

# Convertir a Pandas DataFrame
tweets_hora_pd = tweets_por_hora_completo.toPandas()

# Configurar el índice como la hora para el gráfico
tweets_hora_pd = tweets_hora_pd.set_index('hour')

# Crear gráfico de barras
plt.figure(figsize=(14, 6))
tweets_hora_pd.plot.bar(
    y='tweets_promedio',
    color='steelblue',
    edgecolor='black',
    legend=False
)

# Personalizar el gráfico
plt.title('Patrón de Actividad Horaria en Twitter\n(Promedio de tweets por hora del día)', 
          fontsize=16, fontweight='bold')
plt.xlabel('Hora del día', fontsize=12)
plt.ylabel('Promedio de tweets', fontsize=12)
plt.xticks(rotation=0)
plt.grid(axis='y', alpha=0.3, linestyle='--')
plt.tight_layout()

# Mostrar el gráfico
plt.show()

# Mostrar estadísticas adicionales
print("\n=== ESTADÍSTICAS DE ACTIVIDAD HORARIA ===")
print(f"Hora con mayor actividad: {tweets_hora_pd['tweets_promedio'].idxmax()}:00 horas")
print(f"Tweets en hora pico: {tweets_hora_pd['tweets_promedio'].max():.0f}")
print(f"Hora con menor actividad: {tweets_hora_pd['tweets_promedio'].idxmin()}:00 horas")
print(f"Tweets en hora valle: {tweets_hora_pd['tweets_promedio'].min():.0f}")
print(f"Promedio general: {tweets_hora_pd['tweets_promedio'].mean():.0f} tweets/hora")
```


![[Pasted image 20251105231131.png]]
> === ESTADÍSTICAS DE ACTIVIDAD HORARIA ===
> Hora con mayor actividad: 23:00 horas
> 	Tweets en hora pico: 625900
> Hora con menor actividad: 5:00 horas
> 	Tweets en hora valle: 35400
> Promedio general: 266929 tweets/hora



### Estratificado

En muchas ocasiones el sampling homogéneo no es adecuado ya que por la propia estructura de los datos determinados segmentos pueden estar sobre-representadas. Este es el caso que observamos en los tweets donde las grandes áreas urbanas están sobrerepresentadas si lo comparamos con el volumen de población. En esta actividad vamos a ver cómo aplicar esta técnica al dataset de tweets, para obtener un sampling que respete la proporción de diputados por provincia.

En España, el proceso electoral asigna un volumen de diputados a cada provincia que depende de la población y de un porcentaje mínimo asignado por ley. En el contexto Hive que hemos creado previamente (```spark```) podemos encontrar una tabla (```province_28a```) que contiene información sobre las circunscripciones electorales. Cargad ésta tabla en una variable con nombre ```province```.  Cargad esta tabla en una variable con nombre ```province```.


```python
# YOUR CODE HERE
raise NotImplementedError()

province.limit(20).show()
assert province.count() == 52, "Incorrect answer"
```

Para hacer un sampling estratificado lo primero que tenemos que hacer es determinar la fracción que queremos asignar a cada categoría. En este caso queremos una fracción que haga que la ratio tweets diputado sea igual para todas las capitales de provincia. Debemos tener en cuenta que la precisión de la geolocalización en Twitter es normalmente a nivel de ciudad. Por eso, para evitar incrementar la complejidad del ejercicio, vamos a utilizar los tweets en capitales de provincia como proxy de los tweets en toda la provincia.

## Paso 7.2 — Extraer el nombre del lugar

Primero extraemos la subcolumna `place.name` para tenerla como columna de texto normal.  
Esto simplifica la unión posterior con `province.capital`.

```python
from pyspark.sql.functions import col

# Extraer el nombre de la ciudad o lugar
tweets_place = tweets_geo.select(
    col("_id"),
    col("created_at"),
    col("lang"),
    col("place.name").alias("place_name"),
    col("text")
)

print(f"Total de tweets con 'place_name': {tweets_place.count()}")
tweets_place.show(10, truncate=False)
```

```python
# Unir los tweets con la tabla de provincias (por capital)
tweets_province = (tweets_place
    .join(province, tweets_place.place_name == province.capital, "inner")
    .select("place_name", "province", "capital", "ccaa", "diputados", "created_at", "text")
)

print(f"Tweets con provincia identificada: {tweets_province.count()}")
tweets_province.show(10, truncate=False)
```

```python
# Contar tweets por provincia
tweets_por_provincia = (tweets_province
    .groupBy("province")
    .count()
    .withColumnRenamed("count", "tweets_total")
    .orderBy("tweets_total", ascending=False)
)

print("\nTweets totales por provincia:")
tweets_por_provincia.show(10)
```


> La tabla province contiene 52 provincias.
> 
> Con las siguientes columnas: ['_id', 'created_at', 'lang', 'place', 'retweeted_status', 'text', 'user'].
> 
> root
>  |-- capital: string (nullable = true)
>  |-- province: string (nullable = true)
>  |-- ccaa: string (nullable = true)
>  |-- population: long (nullable = true)
>  |-- diputados: long (nullable = true)
> 
> Primeras filas de la tabla province:
> +-----------+-----------+------------------+----------+---------+
> |    capital|   province|              ccaa|population|diputados|
> +-----------+-----------+------------------+----------+---------+
> |     Teruel|     Teruel|            Aragón|     35691|        3|
> |      Soria|      Soria|   Castilla y León|     39112|        2|
> |    Segovia|    Segovia|   Castilla y León|     51683|        3|
> |     Huesca|     Huesca|            Aragón|     52463|        3|
> |     Cuenca|     Cuenca|Castilla-La Mancha|     54898|        3|
> |      Ávila|      Ávila|   Castilla y León|     57697|        3|
> |     Zamora|     Zamora|   Castilla y León|     61827|        3|
> |Ciudad Real|Ciudad Real|Castilla-La Mancha|     74743|        5|
> |   Palencia|   Palencia|   Castilla y León|     78629|        3|
> | Pontevedra| Pontevedra|           Galicia|     82802|        7|
> +-----------+-----------+------------------+----------+---------+
> only showing top 10 rows
> 
> El objetivo del _sampling estratificado_ es que la **ratio de tweets por diputado sea la misma** en todas las provincias.  
> Es decir, que el número de tweets seleccionados en cada provincia sea proporcional a su número de diputados.
> 
> Para ello:
> 
> 1. Calculamos cuántos tweets hay en cada provincia.
> 2. Unimos esa tabla con la de diputados (`province_28a`).
> 3. Definimos las fracciones de muestreo de forma proporcional.


---

##  Paso 7.5 — Calcular fracciones de muestreo proporcionales a los diputados

Queremos que la **ratio de tweets/diputado sea igual en todas las provincias**,  
por lo que las fracciones de muestreo deben ser proporcionales a:

[  
\text{fracción}_i = k \times \frac{\text{diputados}_i}{\text{tweets_total}_i}  
]

donde ( k ) es una constante de escala (por ejemplo, 0.01 para un tamaño de muestra similar al 1%).

```python
from pyspark.sql.functions import lit

# Unir con la tabla de provincias para acceder al número de diputados
tweets_diputados = (tweets_por_provincia
    .join(province.select("province", "diputados"), on="province", how="inner")
    .withColumn("fraction", lit(0.01) * col("diputados") / col("tweets_total"))
)

# Mostrar algunas fracciones
tweets_diputados.select("province", "diputados", "tweets_total", "fraction").show(10)
```


> [Stage 148:=================================================>       (7 + 1) / 8]
> 
> +---------+---------+------------+--------------------+
> | province|diputados|tweets_total|            fraction|
> +---------+---------+------------+--------------------+
> |   Málaga|       11|         546|2.014652014652014...|
> |  Badajoz|        6|         147|4.081632653061224E-4|
> |   Madrid|       37|        4911|7.534107106495622E-5|
> | Asturias|        7|         329|2.127659574468085...|
> |   Cuenca|        3|          39|7.692307692307692E-4|
> |   Burgos|        4|         130|3.076923076923077E-4|
> |Cantabria|        5|         189|2.645502645502645...|
> |   Murcia|       10|         461|2.169197396963123...|
> |  Vizcaya|        8|         225|3.555555555555555...|
> |    Álava|        4|         105|3.809523809523809...|
> +---------+---------+------------+--------------------+
> only showing top 10 rows
> 

---

##  Paso 7.6 — Crear diccionario de fracciones y aplicar _sampling estratificado_

```python
# Convertir las fracciones a diccionario
fractions_dict = {row["province"]: row["fraction"] for row in tweets_diputados.collect()}

# Aplicar sampling estratificado
tweets_sample_stratified = tweets_province.sampleBy(
    col="province",
    fractions=fractions_dict,
    seed=42
)

print(f"Total de tweets en el muestreo estratificado: {tweets_sample_stratified.count()}")
```






---

## 🕒 Paso 7.7 — Repetir el análisis horario con la muestra estratificada

Ahora puedes repetir el mismo análisis que hiciste en el muestreo homogéneo,  
solo cambiando el dataset de entrada a `tweets_sample_stratified`.

```python
from pyspark.sql.functions import hour, date_format

# Crear tabla con hora y día
tweets_timestamp_stratified = (tweets_sample_stratified
    .withColumn("hour", hour("created_at"))
    .withColumn("day", date_format("created_at", "MM-dd-yy"))
    .select("created_at", "hour", "day")
    .orderBy("created_at", ascending=True)
)

# Agrupar por hora
tweets_por_hora_stratified = (tweets_timestamp_stratified
    .groupBy("hour")
    .count()
    .withColumnRenamed("count", "tweets")
    .orderBy("hour")
)

# Convertir a Pandas y graficar
tweets_hora_stratified_pd = tweets_por_hora_stratified.toPandas().set_index("hour")

import matplotlib.pyplot as plt

plt.figure(figsize=(14,6))
tweets_hora_stratified_pd.plot.bar(
    y="tweets", color="darkorange", edgecolor="black", legend=False
)
plt.title("Patrón de Actividad Horaria en Twitter (Muestreo Estratificado)", fontsize=16)
plt.xlabel("Hora del día", fontsize=12)
plt.ylabel("Tweets", fontsize=12)
plt.xticks(rotation=0)
plt.grid(axis="y", alpha=0.3, linestyle="--")
plt.tight_layout()
plt.show()
```
