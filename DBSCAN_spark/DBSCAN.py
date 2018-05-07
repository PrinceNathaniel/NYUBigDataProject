rom csv import reader
import sys
import numpy as np
import pandas as pd
import heapq
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import time
import seaborn as sb
from pylab import rcParams
from sklearn.cluster import DBSCAN
from sklearn import metrics
from pyspark import SparkContext
from csv import reader

data = sc.textFile("/FileStore/tables/welldataset.csv")
data = data.mapPartitions(lambda x: reader(x, delimiter='\t'))
data.collect()
header = data.first() 
data = data.filter(lambda row : row != header)

data_df = spark.createDataFrame(data)
data_pd = data_df.toPandas()

test_data = data_pd[:5]

print(test_data)

model = DBSCAN(eps = 1000, min_samples = 3).fit(data_pd)
print(model)
outliers_df = pd.DataFrame(data_pd)
outliers = outliers_df[model.labels_ == -1]
print outliers

total_rows = outliers.shape
print total_rows[0]
