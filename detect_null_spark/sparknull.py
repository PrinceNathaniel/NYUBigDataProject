import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1], 1)

lines = lines.mapPartitions(lambda x: reader(x, delimiter='\t'))

data_set = lines.map(lambda x: (x))

dataRDD = spark.createDataFrame(data_set)

collect_data = dataRDD.collect()

def NullFilter(l):
    return l != '' and l != '""'

def filtered(l):
    filtered_ = sc.parallelize(l).filter(Nullfilter).collect()
    return len(filtered_)

null_list = []

def filteredRDD(l):
  for i in range(len(l)):
    if(filtered(l[i]) != len(l[i])):
      null_list.append(i)

filteredRDD(collect_data)
