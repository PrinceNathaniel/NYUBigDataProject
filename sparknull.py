import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1], 1)

lines = lines.map(lambda x: x.split('\t'))

lines_1 = lines.map(lambda x: (x[0], x[1], x[2]))

lines1_RDD = spark.createDataFrame(lines_1)

collect_1 = lines1_RDD.collect()

data_set = lines.map(lambda x: (x))

dataRDD = spark.createDataFrame(data_set)

