import sys
import numpy as np
from pyspark import SparkContext
from csv import reader


sc = SparkContext()

lines = sc.textFile(sys.argv[1], 1)

lines = lines.map(lambda x: x.split('\t'))
### if the header is included in lines ###
# lines=lines.filter(lambda line x:x!=lines.first())
n_rows=len(lines.collect())
n_element=len(lines.first())

lines=lines.cartesian(lines)

def distance(x):
	dist=0
	for i in range(n_element):
		dist+=(float(x[0][i])-float(x[1][i]))**2
	return np.sqrt(dist)
dist=lines.map(lambda x:distance(x))
