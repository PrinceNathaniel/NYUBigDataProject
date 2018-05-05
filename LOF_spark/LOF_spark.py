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
from pyspark.sql.functions import broadcast
display(dbutils.fs.ls("/FileStore/tables/"))

data = sc.textFile("/FileStore/tables/5c5x_3qz9-5a564.tsv")
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
lines = df.rdd.map(list).zipWithIndex()
lines = lines.map(lambda (key, index): (index,key))

n_rows=len(lines.collect())
n_element=len(lines.first())

lines=lines.cartesian(lines).sortByKey()
lines.repartition(10)
def distance(x):
	dist=0
	for i in range(n_element):
		dist += (float(x[0][1][i])-float(x[1][1][i]))**2
	return np.sqrt(dist)
dist=lines.map(lambda x:distance(x))
res = dist.collect()

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
      res = []
      for j in range(0,n):
        res.append((l[i+j],j))
      yield res
partition = sc.parallelize(list(chunks(res, n_rows)))

k = 20
def kdistance(entry,k):
  entry = sorted(entry, key=lambda x: x[0] ,reverse = False)
  neighboures = []
  neighboures_distance = []
  for i in range(1, k+1):
    neighboures.append(entry[i][1])
    neighboures_distance.append(entry[i][0])
  return [entry[k][0],neighboures,neighboures_distance]

kdistanceres= partition.map(lambda x:kdistance(x,k)).zipWithIndex().collect()

def add_kdiatanceofO(kdistanceres):
  for i in range(len(kdistanceres)):
    kdiatanceofO = []
    for j in kdistanceres[i][0][1]:
      kdiatanceofO.append(kdistanceres[j][0][0])
    kdistanceres[i][0].append(kdiatanceofO)

add_kdiatanceofO(kdistanceres)

def reach_distance(d1,d2):
  return max(d1,d2)

def lrd(entry,k):
  sum_reach_distance = 0
  for i in range(k):
    sum_reach_distance += reach_distance(entry[0][2][i], entry[0][3][i])
  return ([k/sum_reach_distance,entry[0][1] ], entry[1])

lrd_rdd = kdistanceres_rdd.map(lambda x:lrd(x,k))
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
lrd = lrd_rdd.map(lambda x:lof(x,k)).collect()

def n_outlier(entry,n):
  entry = sorted(entry, key=lambda x: x[0] ,reverse = True)
  return entry[0:n]
outlier = n_outlier(lrd,10)