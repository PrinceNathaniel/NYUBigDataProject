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
#one-hot encoding
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

lines = df.rdd.map(list)
n_rows=len(lines.collect())
n_element=len(lines.first())

lines=lines.cartesian(lines)

def distance(x):
    dist=0
    for i in range(n_element):
        dist += (float(x[0][i])-float(x[1][i]))**2
    return np.sqrt(dist)
dist=lines.map(lambda x:distance(x))
res = dist.collect()

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
res = sc.parallelize(list(chunks(res, n_rows)))

def kdistance(entry,k):
  entry.sort()
  return entry[k]
kdistanceres = res.map(lambda x:kdistance(x,200)).zipWithIndex().collect()


def n_outlier(entry,n):
  entry = sorted(entry, key=lambda x: x[0] ,reverse = True)
  return entry[0:n]
outlier = n_outlier(kdistanceres,10)
outlier
