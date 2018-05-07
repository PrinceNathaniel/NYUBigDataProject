from sklearn.preprocessing import OneHotEncoder
from sklearn import preprocessing
from csv import reader
import sys
import numpy as np
import pandas as pd
import heapq
from pyspark import SparkContext
from csv import reader
from sklearn.neighbors import KDTree

# 读取数据, 把header去掉
data = sc.textFile("/FileStore/tables/5c5x_3qz9-5a564.tsv")
data = data.mapPartitions(lambda x: reader(x, delimiter='\t'))
data.collect()
header = data.first() 
data = data.filter(lambda row : row != header)
temp_data = data

# 转DF
data_df = spark.createDataFrame(temp_data)
data_df.show()

# 转PandasDF
temp_pd = data_df.toPandas()
temp_pd

# 判断是否位数字
def is_digit(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

# 确定哪些列有字符串，哪些列就需要one-hot-encoding
drop_list = []
# sum_list = data_df.groupby('_2').sum().rdd.sortBy(lambda x: x[0]).collect()
for i in range(len(header)):
  sum_list = data_df.groupby('_' + str(i + 1)).sum().rdd.collect()
  for j in range(len(sum_list)):
    if not is_digit(sum_list[j][0]):
      drop_list.append(i)
      break
print drop_list

# 去除带有字符串的列
data_df = spark.createDataFrame(temp_data)
for i in drop_list:
  print i
  data_df = data_df.drop('_' + str(i+1))
temp_df2 = data_df.toPandas()
temp_df2

# 选出来需要做 one-hot-encoding 的 列, 先做LabelEncoder, 再做OneHotEncoder, 把新列加入
new_pd = temp_pd.icol(drop_list)
le = preprocessing.LabelEncoder()
x_3 = new_pd.apply(le.fit_transform)
enc = preprocessing.OneHotEncoder()
enc.fit(x_3)
onehotlabels = enc.transform(x_3).toarray()
onehot_array = onehotlabels
one_hot_transpose = onehot_array.transpose() 

for i in range(one_hot_transpose.shape[0]):
  temp_df2['new_' + str(i)] = one_hot_transpose[i]

temp_df2

sdf = spark.createDataFrame(temp_df2)

sdf.show()

