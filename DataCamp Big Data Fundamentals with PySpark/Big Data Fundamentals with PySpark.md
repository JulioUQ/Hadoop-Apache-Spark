## Index

**Chapter 1:** Fundamentals of BigData and introduction to Spark as a distributed computing framework.
	Main Components: Spark Core, and Spark built-in libraries - Spark SQL, Spark MLlib, Graphx, and Spark Streaming.
	 PySpark: Apache Spark's Python API to execute Spark jobs
	 PySpark shell: For developing the interactive applications in python
	 Spark modes: Local and cluster mode.
Chapter 2: Introduccion to RDDs, different features of RDDs, methods of creating RDDs and RDD operations (Tranformations and Actions).
Chapter 3: Introducction to Spark SQL, Dataframe abstraction , creating DataFrames, DataFrames oeprations and visualizing Bid Data throught DataFrames.
Chapter 4: Introduction to Spark MLlib, the three C's of Mchine Learning (Collaborative filtering, Classification and Clustering).

# Context

There's been a lot of buzz about Big Data over the past few years, and it's finally become mainstream for many companies. But what is this Big Data? This course covers the fundamentals of Big Data via PySpark. Spark is a "lightning fast cluster computing" framework for Big Data. It provides a general data processing platform engine and lets you run programs up to 100x faster in memory, or 10x faster on disk, than Hadoop. You’ll use PySpark, a Python package for Spark programming and its powerful, higher-level libraries such as SparkSQL, MLlib (for machine learning), etc. You will explore the works of William Shakespeare, analyze Fifa 2018 data and perform clustering on genomic datasets. At the end of this course, you will have gained an in-depth understanding of PySpark and its application to general Big Data analysis.
# What is BigData?

Big data is a term used to refer to the sudy and applications of data sets that are too complex for traditional data-procssing software.

# The 3 V's of BigData

Volume: 
- Size of the data
- Different souces and formats
- Speed of the data
	
Variety
Velocity

# Big data concepts and Terminology

Clustered computing: Collection of resources of multiple machines
Parallel computing: Simultaneous computation on single computer.
Distributed computing: Collection of nodes (networked computers) that run in parallel.
Batch processing: Breaking the job into small pieces and running them on individual machines.
Real-time processing: Inmediate processing of data.

# Big Data processing systems

Hadoop/MapReduce: Scalable and fault tolerant framework written in Java
- Open Source
- Batch processing

Apache Spak: General purpose and ligthnign fast cluster computing system
- Open Source
- Both batch and real-time processing

> Note: ApacheSpark is nowadays preferred over Hadoop/MapReduce


# Feautres of Apache Spark framework

Distributed cluster ocmputing framework
Efficient in-memory ocmputtaions for large data sets.
Lightning fast dta processinf framework
Provides support for Java, Scala, Python, R and SQL.

# Apache Spark Components

SparkSQL
MLlib (MachineLearning)
GrapthX
SparkStreaming
RDD API ApacheSpark Core

# Spark modes of deployment

Local mode: Single machine such as your laptop
- Local mode convenient for testing, debugging and demostration.
Cluster mode: Set of pre-defined machines
- Good for production
Workflow: Local -> clusters
No code change neccesary

---

# PySpark: Spark with python

## Overview of PySpark

Apache Spark is written in Scala
To support Python with Spark, Apache Spark Community released PySpark
Similar computation speed and power as Scala
PySpark API's are similar to Pandas and Scikit-learn

## What is Spark shell?

Interactive environment for runnign Spark jobs
Helpful for fast interactive prototyping
Spark shells allow interacting with data on disk or in memory
Three different Spark shells:
- Spark-shell for Scala
- PySpark-shell for Python
- SparkR for R

## PySpark shell

PySpark shell is the Python-based command line tool
PySpark shell allows data scientists interface with Spark data structures
PySpark shell support connecting to a cluster


## Understanding SparkContext

SparkContext is an entry point into the world of Spark
An entry point is a way of connecting to Spark cluster
An entry point is like a key to the house
PySpark has a default SparkContext called `sc`


## Inspecting Spark Context

Version: To retrieve SparkContext version `sc.version`
Python Version: To retrieve Python version of SparkContext `sc.pythonVer`
Master: URL of the cluster or local string to run in local mode of SparkContext `sc.master`

## Loading data in PySpark

SparkContext's `parallelize()` metod
- `rdd = sc.parallelize([1,2,3,4,5])`
SparkContext's `textFile()` method
- `rdd2 = sc.textFile("test.txt")`

# Use of Lambda functions in python - filter()

## What are anonymous functions in Python?

Lambda functions are anonymous functions in Python
Very powerful and used in Python. Quite efficient with `map()` and `filter()`
Lambda functions create functions to be called later similar to `def`
It returns the functions without any name (i.e. anonymous)
Inline a function definition or to defer execution of a code

## Lambda function syntax

The general form of lambda function is 
`lambda argument: expression`
Example of lambda function
```python
double = lambda x: x*2
print(double(3)
```

## Difference between def vs lambda functions

Python code illustrate cube of a number

```python
def cube(x):
	return x**3
	
g = lambda x: x**3

print(g(10))
print(cube(10))
```

No return statement of lambda
Can put lambda function anywhere

## Use of lambda function in Python - map()


`map()` applies a function to all items in the input list
General systax of map() `map(function, list)`
Example map()
```python
items = [1,2,3,4]
list(map(lambda x: x + 2, items))
```

## Use of lambda function in Python - filter()


filter()` function takes a function and a list and returns a new list which the function evaluates as true
General systax of filter() `filter(function, list)`
Example map()
```python
items = [1,2,3,4]
list(map(filter x: x%2 != 0, items))
```


# Abstracting Data with RDDs

## What are RDD's?

Resilient Distributed Datasets
- Resilient: Ability to withstand failures
- Distributed: Spanning across multiple machines
- Datasets: Collection of partitioned data e.g, Arrays, Tables, Tuples etc.,

![[Pasted image 20251021111646.png|500]]


## Creating RDD's How to do it?

Parallelizing an existing colleciton of objects
External datasets:
- Files HDFS
- Objects in Amazon S3 buckets
- lines ina text file
From existing RDDs

## Parallelized collection (parallelizing)
parallelize() for creating RDDs from python lists
```python
numRDD = sc.parallelize([1,2,3,4])
helloRDD = sc.parallelize("Hello world")
type(helloRDD)
```
## From external datasets

textFile() for creating RDDs from external datasets

```python
fileRDD = SC.textFile("README.md")
type(fileRDD)
```
## Understanding Partitioning in PySpark

A partition is a logical division of a large distributed data set
parallelize() method
```python
numRDD = sc.parallelize(rango(10), minPartitions = 6)
```
textFile() method

```python
fileRDD = SC.textFile("README.md", minPartitions = 6)
type(fileRDD)
```
The number of partitions in an RDD can be found by using `getNumPartitions()` method


# Basic RDD Transformations and Actions

Transformrations create new RDDs, while Actions perform computtiaon on the RDDs.
Basic RDD Transformations are: map(), filter(), flatMap() and union().

![[Pasted image 20251021113943.png]]

- **map() Transformation**

map() transformation applies a function to all element in the RDD
```python
RDD = sc.parallelize([1,2,3,4])
RDD_map = RDD.map(lambda x: x*x)
```

-  **filter() Transformation**

Filter trnsformation returns a new RDD with only the elemens that pass the condition
```python
RDD = sc.parallelize([1,2,3,4])
RDD_filter = RDD.filter(lambda x: x>2)
```

- **flatMap() Transformation**

flatMap() transformation return multiples values for each element in the original RDD
```python
RDD = sc.parallelize(["Hello world", "How are you])
RDD_flatmap = RDD.flatMap(lambda x: x.split(" "))
```

- **union() Transformation**

```python
inputRDD = sc.textFile("logs.txt")
errorRDD = inputRDD.filter(lambda x: "error" in x.split(" "))
warningsRDD = inputRDD.filter(lambda x: "warnings" in x.split(" "))
combinedRDD = errorRDD.union(warningsRDD)
```

## RDD Actions

They are operations that return a value after running a computation on the RDD
Basic RDD Actions are:

- **collect()**: return all the elements of the data set as an array
```python
RDD_map.collect()
```
- **take(N)**: returns an array with the first N elements of the datset.
```python
RDD_map.take(2)
```
- first() -> similar to take(1)
```python
RDD_map.first()
```
- **count()**: return the number of elements in the RDD
```python
RDD_map.count()
```

# Pair RDDs in PySpark

## Introduction to pair RDDs in PySpark

Real life datasets are usually key/value pairs
Each row is a key and maps to one or more values
Pais RDD is a special data structure to work with this kind of datasets
Pais RDD: Key is the identifier and value is the data

## Creating pair RDDs

Two common ways to create pair RDDs
- From a list of key-value tuple
- From a regular RDD
Get the data into key/value form for paired RDD

```python
my_tuple =[('Sam', 23), ('Mary', 34), ('Peter', 25)]
pairRDD_tuple = sc.parallelize(my_tuple)

my_tuple =[('Sam', 23), ('Mary', 34), ('Peter', 25)]
regularRDD = sc.parallelize(mylist)
pairRDD_RDD = regularRDD.map(lambda s: (s.split(' ')[0], s.split(' ')[1]))
```

## Transformations on pair RDDs

All regular transformations work on pair RDD
Have to pass functions that operate on key value pairs rather than on indivual elements
Examples of paired RDD Transformations:

- reduceByKey() transformation combines values with the same key
	- It runs parallel operations for each key in the dataset
	- It is a transformation and not action

```python
regularRDD = sc.parallelize([('Messi', 23), ('Ronaldo', 34), ('Neymar', 22), ('Messi', 24)]
pairRDD_reducebykey = regularRDD.reduceByKey(lambda x, y: x + y)
pairRDD_reducebykey.collect()
```

- sortByKey() operation orders pair RDD by key.
	- It returns an RDD sorted by key in ascending or descending order.

```python
pairRDD_reducebykey_rev = pairRDD_reducebykey.map(lambda x: (x[1], x[0]))
pairRDD_reducebykey_rev.sortByKey(ascending=False).collect()
```
- groupByKey() groups all the values with the same key in the pair RDD
```python
airports = [('US', 'JFK'), ('UK', 'LHR'), ('FR', 'CDG'), ('US', 'SFO')]
regularRDD = sc.parallelize(airports)
pairRDD_group = regularRDD.groupByKey().collect()

for cont, air in pairRDD_group:
	print(cont, list(air))
```

- join() transformation joins the two pair RDDs based on their key
- ```python
  RDD1 = sc.parallelize([('Messi', 34), ('Ronaldo', 32), ('Neymar', 24)])
  RDD2 = sc.parallelize([('Messi', 100), ('Ronaldo', 80), ('Neymar', 120)])
  
  RDD1.join(RDD2).collect()
  ```


# Adavanced RDD Actions

## reduce() action
- reduce(func) action is used for aggregating the element of a regular RDD
- The function should be commutatitive (changing the order of the operands does not change the resutl) and associative.
- An example of  `reduce()` action in PySpark.

```python
x = [1, 3, 4, 6]
RDD = sc.parallelize(x)
RDD.reduce(lambda x, y : x + y)
```
## saveAsTextFile() action
- `saveAsTextFile()` action saves RDD into a text file inside a directory with each partition as a separate file.
```python
RDD.saveaAsTextFile("tempFile")
```
- `coalesce()` method can be used to save RDD as a single text file
```python
RDD.coalesce(1).saveAsTextFile("tempFile")
```

## Actions Operations on pair RDDs
- RDD actions available for PySpark pair RDDs
- Pair RDD actions leverage the key-value data
- Few examples of pair RDD actions include:

### countByKey() action

- `countByKey()` only available for type (K, V)
- `countByKey()` action counts the number of elements for each key
- Example of `countByKey()` on a simple list

```python
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
for key, val in rdd.countByKey().items():
print(key, val)
```

###  collectAsMap() action

- `collectAsMap()` return the key-value pairs in the RDD as a dictionary
- Example of `collectAsMap()` on a simple tuple
```python
sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
```


# Abstracting Data with DatFrames

## What are PySpark DatFrames?

- PySpark SQL is a Spark library fo structure data. It provides more information about the structure of data and computation.
- PySpark DataFrame is an immutable distributed collection of data with named columns.
- Designed for processing both structured (e.g. relational database) and semi-structured data (e.g JSON)
- Dataframe API is available in Python, R, Scala and Java
- DataFrames in PySpark support both SQL queries (`SELECT * FROM table`) or expression methods (`df.select()`).

## SparkSession - Entry point for DataFrame API

- SparkContext is the main entry point for creatin rDDs
- SparkSession provides a single point of entry to interact with Spark DataFrames
- SparkSession is used to create DataFrame, register DataFrames, execute SQL queries
- SparkSession is available in PySpark shell as spark

## Creating DataFrames in PySpark
- Two different methods of creating DatFrames in PySpark
	- From existing RDDs using SparkSession's createDataFrames() method
	- From various data sources (CSV, JSON, TXT) using SparkSession's read method
- Schema controls the data and helps DataFrames to optimize queries
- Schema provides information about columns name, type of data in the column, empty values etc.,

## Create a Data from RDD

```python
iphones_RDD 0 sc.parallelize([tuples])

names = [list_of_columns]

iphones_df = spark.createDataFrame(iphones_RDD, schema=names)
type(iphones_df)
```


## Create a DataFrame from reading a csv/json/txt

```python
df_csv = spark.read.csv("peaople.csv", header = True, inferSchema=True)

df_json = spark.read.json("people.json")

df_txt = spark.read.txt("people.txt")
```

- Path to the file and two optional parameters
- Two optional parameters
	- header = True, inferSchema = True


# Operating on DataFrames in PySpark

## DataFrame operators in PySpark
- DataFrame operations: Transformations and Actions
- DataFrame Transformations:
	- select(), filter(), groupby(), orderby(), dropDuplicates(), and withColumnRenamed()
- DataFrames Actions:
	- head(), show(), count(), columns, and describe()

## select() and show() operations
- `select()` transformation subsets the columns in the DataFrame.
- `show()` actions prints first 20 rows in the DataFrame
## filter() and show() operations
- `filter()` transformation filter out the rows based on a condition
## groupby() and count() operation
-  `groupby()` operation can be used to group a variable
 
## orderby() transformation
- `orderby()` operation sorts the DataFrame based on one or more columns

## dropDuplicates() transformation
- `dropDuplicates()` removes the duplicate rows of a DataFrame

## withColumnRenamed Transformation
- `withColumnRenamed()` renames a column in the DataFrame

## printShema() method
- `printSchema()` operation prints the types of columns in the DataFrames.

## columns actions
- `columns` operations prints the columns of a DataFrame
## describe() actions
- `describe()` operation compute summary of the statistic from numeric columns


# Interacting with DataFrames using PySpark SQL

##  DataFrame API vs SQL queries

- In PySpark You can interact with SparkSQL through DataFrame API and SQL queries
- The DataFrame API provides a programmatic domain-specific language (DSL) for data
- DataFrame transformations and actions are easier to construct programmatically
- SQL query can be concise and easier to understand and portable
- The operations on DataFrames can also be don using SQL queries

## Executing SQL Queries

- The SparkSession `sql()` method executes SQL query
- `sql()` method takes a SQL statement as an argument and returns the results as DataFrame

```python
df.createOrReplaceTempView("table1")

df2 = spark.sql("SELECT col1, col2 FROM table1")
df2.collect()
```

## SQL query to extract data

```python
test_df.createOrReplaceTempView("test_table")

query = '''SELECT Product_ID FROM test_table'''

test_product_df = spark.sql(query)
test_product_df.show(5)
```


## Summarizing and grouping data using SQL queries

```python
test_df.createOrReplaceTempView("test_table")

query = '''SELECT Age, max(Purchase) FROM test_table GROUP BY Age'''

spark.sql(query).show()
```

## Filtering columns using SQL queries

```python
test_df.createOrReplaceTempView("test_table")

query = '''SELECT Age, Purchase, Gender FROM test_table Where Gender == "Female"'''

spark.sql(query).show(5)
```


# Data Visualization in PySpark using DataFrames

## What is Data visualization?
- Data visualization is a way of representing your data in graph or charts
- Open source plotting tools to aid visualization in Python:
	- Matplotlib, Seaborn, Bokeh, etc.,
- Plotting graphs using PySpark DataFrames is done using three metods
	- pyspark_dist_explote library
	- toPandas()
	- HandySpark library

## Data Visualization using Pyspark_dist_explote
- `pyspark_dist_explote` library provides quick insight into DataFrames
- Currently three funcions available: `hist()`, `distplot()` and `pandas_histogram()`

```pyhon
test_df = spark.read.csv("test_csv", header = True, inferSchema=True)

test_df_age = test_df.select('Age')

hist(test_df_age, bins=20, color="red")
```

## Using Pandas for plotting DataFrames
- It's eas to create chars from pandas DataFrames

```pyhon
test_df = spark.read.csv("test_csv", header = True, inferSchema=True)

test_df_sample_pandas = test_df.toPandas()

test_df_sample_pandas.hist('Age')
```

> When you have large volume of data, using `toPandas()` isn't recommended


## Pandas DataFrame vs PySpark DataFrame


- Pandas DataFrames are in-memory, single-server based structures and operations on PySpark run in parallel
- The result is generated as we apply any operation in Pandas whereas operations in PySpark DataFrame are lazy evaluation
- Pandas DataFrame as mutable and PySpark DataFrames are immutable
- Pandas API support more operations than PySpark DataFrame API.

## HandySpark method of visualization

- HandySpark is a package designed to improve PySpark user experience
	- Easy data fetching
	- Distributed computation retained

```
test_df = spark.read.csv("test_csv", headder=True, inferSchema=True)

hdf = test_df.toHandy()

hdf.cols["Age"].hist()
```

# Overview of PySpark MLlib

## What is PySpark MLlib?

- Machine learning is a scientific discipline that explores the construction and study of algorithms that can learn from data.
- MLlib is a component of Apache Spark for machine learning
- Various tools provided by MLlib include:
	- ML Algorithms: collaborative filtering, classification, and clustering
	- Featurization: feature extraction, transformation, dimensionality reduction, and selection
	- Pipeline: tools for constructing, evaluating, and tuning ML Pipelines.

## Why PySpark MLlib?

- Scikit-learn is a popular Python library for data mining and machine learning.
- Scikit-learn algorithms only work for small datasets on a single machine
- Spark's MLlib algorithms are designed for parallel processing on a cluster.
- Supports languages such as Scala, Java, and R
- Provides a high-level API to build machine learning pipelines

## PySpark MLlib Algorithms
- Classification (Binary and Multiclass) and Regression: Linear SVMs, logistic regression, decision trees, random forests, gradient-boosted trees, naive Bayes, linear least squares, Lasso, ridge regression, isotonic regression.
- Collaborative filtering: Alternating least squares (ALS)
- Clustering: K-means, Gaussian mixture, Bisecting K-means and Streaming K-Means.

## The three C's of machine learning in PySpark MLlib
- Collaborative filtering (recommeder engines): Produce recomendations.
- Classification: Identifying to wich of a set of categories a new observation belong
- Clustering: Groups data based on similar characteritics

## PySpark MLlib imports
- Collaborative filtering
```python
from pyspark.mllib.recommendation import ALS
```
- Classification
```python
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
```
- Clustering
```python
from pyspark.mllib.clustering import KMeans
```


# Collaborative filtering

## What is Collaborative filtering?

- Collaborative filtering is finding users that share common interest
- Collaborative filtering is commonly used for recommender systems
- Collaborative filtering approaches:
	- User-User Collaborative filtering: Finds users that are similar to the target user
	- Item-Item Collaborative filtering: Finds and recommends items that are similar to items with the target user.
## Rating clas in pyspark.mllib.recommendation submodule
- The Rating class is a wrapper around tuple (user, product and rating).
- Useful for parsing the RDD and creating a tuple of users, product and rating.
```python
from pyspark.mllib.recommendation import Rating
r = Rating(user = 1, product = 2, rating = 5.0)
(r[0], r[1], r[2])
```

## Splitting the data using randomSplit()
- Splitting data into training and testing sets is importatnt for evaluating predictiv modeling.
- Typically a large portion of data is assigned to training compared to testing data.
- PySpark's `randomSplit()` method randomly splits with the provided weights and returns multiple RDDs
```python
data = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
training, test = data.randomSplit([0.6, 0.4])
training.collect()
test.collect()
```

## Alternating Least Squares (ALS)

- Alternating Least Squares (ALS) algorithm in `spark.mllib` provides collaborative filtering
- `ALS.train(rating, rank, iterations)`
```python
r1 = Rating(1, 1, 1.0)
r2 = Rating(1, 2, 2.0)
r3 = Rating(2, 1, 2.0)

ratings = sc.parallelize([r1, r2, r3])

ragins.collect()

model = ALS.train(ratings, rank=10, iterations=10)
```

## predictAll()
- The predictAll() method returns a list of predicted ratings for input user product pais
- The  method takes in a RDD without ratings to generate the ratings.
```python
unrated_RDD = sc.parallelize([(1, 2), (1, 1)])

predictions = model.predictAll(unrated_RDD)
predictions.collect()
```

## Model evaluation

- The MSE is the average value of the square of `actual rating - predicted rating`

```pyhton
rates = ratings.map(lambda x: ((x[0], x[1], x[2])))
rates.collect()

preds = predictions.map(lambda x: ((x[0], x[1], x[2])))
preds.collect()

rates_preds = rates.join(preds)
rates_preds.collect()

MSE = rates_preds.map(lambda r: r[1][0] - r[1][1]**2).mean()
```

# Classification


## Classification using PySpark MLlib
- Classification is a supervised machine learning algorithm for sorting the input data into different categories. Binary classification or Multi-class classification

## Introduction to Logistic Regression
- Losigtic Regresion predict a binary response based on some variables.

## Working with Vectors
- PySpark MLlib contains specific data types Vectors and LabelledPoint
- Two types of Vectors
	- Dense Vector: store all their entries in a n array of floating point numbers
	- Sparse Vector: store only the nonzero values and their indices
```python
denseVec = Vectors.dense([1.0, 2.0, 3.0])

sparseVec = Vectors.sparse(4, {1: 1.0, 3: 5.5})
```

## LabeledPoint() in PySpark MLlib

- A LabeledPoint is a wrapper for input features and predicted value.
- For binary classification of Logistic Regression, a label is either 0 (negative) or 1 (positive)

## HashingTF() in PySpark MLlib
- `HashingTF()` algorithm is used to map feature value to indices in the feature vector

## Logistic Regression using LogisticRegressionWithLBFGS
- Logistic Regression using Pyspark MLlib is achieved using LogisticRegressionWithLBFGS class

```python
data = [
		LabeledPoint(0.0, [0.0, 1.0]),
		LabeledPoint(1.0, [1.0, 0.0])]
		
RDD = sc.parallelize(data)

lrm = LogisticRegressionWithLBFGS.train(RDD)

lrm.predict([1.0, 0.0])
lrm.predict([0.0, 1.0])
```

# Clustering

## What is Clustering?

- Clustering is the unsupervised learning task to organize a collection of data into group
- PySpark MLlib currently supports the following clustering models
	- K-means
	- Gaussian mixture
	- Power iteration clustering (PIC)
	- Bisecting k-means
	- Streaming k-means

## K-means Clustering

- K-means is the most popular clustering method.

## K-means with Spark MLlib

```python
RDD = sc.textFile("WineData.csv"). \
		map(lambda x: x.split(",")). \
		map(lambda x: [float(x[0]), float(x[1])])

RDD.take(5)
```

## Train a K-means clustering model
- Training K-means model is done using `KMeans.train()` method

```python
from pyspark.mllib.clustering import KMeans

model = KMeans.train(RDD, k = 2, maxIterations = 10)
model.clusterCenters
```

## Evaluating the K-means Model

- Actualmente no hay un metodo para evaluar el modelo por eso construimos a mano la funcion:

```python
from math import sqrt
def error(point):
	center = model.centers[model.predict(point)]
	return sqrt(sum([x**2 for x in (point - center)]))
	
WSSE = RDD.map(lambda point: error(point)).reduce(lambda x, y: x +y)
print("Within Set Sum of Squared Error = " + str(WSSSE))
```


## Visualizing K-means clusters

```python
wine_data_df = spark.createDataFrame(RDD, schema=["col1", "col2"])
wine_data_df_pandas = wine_data_df.toPandas()

cluster_centers_pandas = pd.DataFrame(model.clusterCentrs, columns =["col1", "col2"])

plt.scatter(wine_data_df_pandas["col1"], wine_data_df_pandas["col2"])
plt.scatter(wine_data_df_pandas["col1"], wine_data_df_pandas["col2"], color="red", marker="x")
```






---

