from csv import reader
import sys
from operator import add
from pyspark import SparkContext
import pandas as pd
import heapq
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf,col,when
from pyspark.sql.types import IntegerType
display(dbutils.fs.ls("/FileStore/tables/"))
sc = SparkContext()

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

for i in range(len(header)):
  categories = df.select(header[i]).distinct().rdd.flatMap(lambda x : x).collect()
  tem = dofloat(categories)
  if len(tem) == 0:
    continue
  for category in categories:
      function = udf(lambda item: 1 if item == category else 0, StringType())
      new_column_name = header[i]+'_'+category
      df = df.withColumn(new_column_name, function(col(header[i])))
  df = df.drop(header[i])
df.show()
