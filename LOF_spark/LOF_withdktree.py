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


data = sc.textFile("/FileStore/tables/h9gi_nx95-fba74.tsv")
data = data.mapPartitions(lambda x: reader(x, delimiter='\t'))
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
  neighboures_distance = entry[0][0]
  return [k_neighbor, neighbors,neighboures_distance]

kdistanceres= results.map(lambda x:cleanresult(x,k_neighbors-1)).zipWithIndex().collect()

def add_kdiatanceofO(kdistanceres):
  for i in range(len(kdistanceres)):
    kdiatanceofO = []
    for j in kdistanceres[i][0][1]:
      kdiatanceofO.append(kdistanceres[j][0][0])
    kdistanceres[i][0].append(kdiatanceofO)

add_kdiatanceofO(kdistanceres)

kdistanceres_rdd = sc.parallelize(kdistanceres)
def reach_distance(d1,d2):
  return max(d1,d2)

def lrd(entry,k):
  sum_reach_distance = 0
  for i in range(1,k):
    sum_reach_distance += reach_distance(entry[0][2][i], entry[0][3][i])
  return ([k/sum_reach_distance,entry[0][1] ], entry[1])

lrd_rdd = kdistanceres_rdd.map(lambda x:lrd(x,k_neighbors))
lrd = lrd_rdd.collect()

def add_lrdOfO(lrd):
  for i in range(len(lrd)):
    sum_lrdOfO = 0
    for j in lrd[i][0][1]:
      sum_lrdOfO += lrd[j][0][0]
    lrd[i][0].append(sum_lrdOfO)
add_lrdOfO(lrd)

lrd_rdd = sc.parallelize(lrd)
def lof(entry,k):
  lof = entry[0][2]/k/entry[0][0]
  return [lof,entry[1]]
lrd = lrd_rdd.map(lambda x:lof(x,k_neighbors)).collect()

def n_outlier(entry,n):
  entry = sorted(entry, key=lambda x: x[0] ,reverse = True)
  return entry[0:n]
outlier = n_outlier(lrd,100)