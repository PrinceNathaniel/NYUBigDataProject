import sys
from operator import add
from pyspark import SparkContext
from csv import reader

sc = SparkContext()

lines = sc.textFile(sys.argv[1], 1)

lines = lines.map(lambda x: x.split('\t'))

data_set = lines.map(lambda x: (x))

dataRDD = spark.createDataFrame(data_set)

collect_data = dataRDD.collect()

len_column = len(collect_data)

len_row = len(collect_data[0])

def NullFilter(l):
    return l != '' and l != '""'

def filtered(l):
    filtered_ = sc.parallelize(l).filter(Nullfilter).collect()
    return len(filtered_)

def filteredRDD(l):
    for i in range(len(l)):
        if(filtered(l[i]) != len(l[i])):
            l.remove(l[i])

result = filteredRDD(collect_data)
