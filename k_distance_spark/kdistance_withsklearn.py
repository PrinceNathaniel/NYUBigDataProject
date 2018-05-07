from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestNeighbors
from csv import reader
import sys
import numpy as np
from pyspark import SparkContext
from csv import reader
from operator import add
import pandas as pd
import heapq
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf,col,when
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import isnan

sc = SparkContext()
data = sc.textFile("/FileStore/tables/735p_zed8-3645b.tsv")
data = data.mapPartitions(lambda x: reader(x, delimiter='\t'))
data.collect()
header = data.first() 
data = data.filter(lambda row : row != header)

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False
def dofloat(entry):
  k = []
  for i in range(len(entry)):
    if isfloat(entry[i]) != True:
      k.append(i)
  return k
df = spark.createDataFrame(data, header)

m = df.count()
for i in range(len(header)):
  k = df.filter((df[header[i]] == "") | df[header[i]].isNull() | isnan(df[header[i]])).count()
  null_percent = float(k)/m
  if null_percent > 0.1:
    df = df.drop(header[i])
    continue
  
  categories = df.select(header[i]).sample(withReplacement=False, fraction=0.1).distinct().replace('', '0').rdd.flatMap(lambda x : x).collect()
  tem = dofloat(categories)
  if len(tem) == 0:
    continue
  if len(tem) >10:
    df = df.drop(header[i])
    continue
  categories = df.select(header[i]).distinct().rdd.flatMap(lambda x : x).collect()
  for category in categories:
      function = udf(lambda item: 1 if item == category else 0, StringType())
      new_column_name = header[i]+'_'+category
      df = df.withColumn(new_column_name, function(col(header[i])))
  df = df.drop(header[i])
  
df = df.replace('', '0')
data = df.rdd.map(list)
data_collect = df.rdd.map(list).collect()



k_neighbors = 25
knnobj = NearestNeighbors(n_neighbors = k_neighbors).fit(data_collect)

bc_knnobj = sc.broadcast(knnobj)
results = data.map(lambda x: bc_knnobj.value.kneighbors(x))

def cleanresult(entry,k):
  k_neighbor = entry[0][0][k]
  neighbors = entry[1][0]
  return [k_neighbor, neighbors]

kdistanceres= results.map(lambda x:cleanresult(x,k_neighbors-1)).zipWithIndex().collect()

def n_outlier(entry,n):
  entry = sorted(entry, key=lambda x: x[0][0] ,reverse = True)
  return entry[0:n]
outlier = n_outlier(kdistanceres,10)