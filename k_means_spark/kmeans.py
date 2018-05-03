import pandas as pd
import heapq
import numpy as np
from math import sqrt
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel

def enlarge(df,colname):
    namelist=list(set(df[colname]))
    if len(namelist)==2:
        key1=namelist[0]
        string='bin'+colname
        df[string]=0
        for i in range(len(df)):
            if df[colname][i]==key1:
                df[string][i]=1
        df.drop(colname, axis=1, inplace=True)
    else:
        for name in namelist:
            string=colname+name
            df[string]=0
        for i in range(len(df)):
            key1=df[colname][i]
            keystring=colname+key1
            df[keystring][i]=1
        df.drop(colname, axis=1, inplace=True)


def creatlist(df):
    for colname in df.columns:
        if len(list(set(df[colname]))) == 1:
            continue
        types=(list(set(df[colname]))[0])
        if types=='none':
            types=(list(set(df[colname]))[1])
        if isinstance(types, str):
            enlarge(df,colname)


pdf=pd.read_csv('data/5c5x-3qz9.tsv',sep='\t',header=0)
creatlist(pdf)
df = sqlContext.createDataFrame(pdf)
rdd = df.rdd.map(list)


sdfilt = rdd.filter(lambda x:np.count_nonzero(np.array(x))>0)
vso = sdfilt.map(lambda x: np.array(x))

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


for i in range(1,11):
    clusters = KMeans.train(vso, i, maxIterations=10,initializationMode="random")
    WSSSE = vso.map(lambda point: error(point)).reduce(add)
    print("Within Set Sum of Squared Error, k = " + str(i) + ": " + str(WSSSE))


clusters = KMeans.train(vso, 3, maxIterations=10, initializationMode="random")
for i in range(0,len(clusters.centers)):
    print("cluster " + str(i) + ": " + str(clusters.centers[i]))

clusters = KMeans.train(vso, 4, maxIterations=10, initializationMode="random")
for i in range(0,len(clusters.centers)):
    print("cluster " + str(i) + ": " + str(clusters.centers[i]))



def addclustercols(x):
    point = np.array(x)
    center = clusters.centers[0]
    mindist = sqrt(sum([y**2 for y in (point - center)]))
    cl = 0
    for i in range(1,len(clusters.centers)):
        center = clusters.centers[i]
        distance = sqrt(sum([y**2 for y in (point - center)]))
        if distance < mindist:
            cl = i
            mindist = distance
    clcenter = clusters.centers[cl]
    return tuple(x) +(float(mindist),)

rdd_w_clusts = sdfilt.map(lambda x: addclustercols(x))
rdd_w_clusts.first()


data = sc.textFile('data/5c5x-3qz9.tsv').map(lambda x: x.split('\t'))
header = tuple(pdf.columns)
header = tuple(header) + ('dist',)

schema_sd = spark.createDataFrame(rdd_w_clusts, header)

schema_sd.createOrReplaceTempView("sd")

spark.sql("SELECT * FROM sd ORDER BY  dist desc LIMIT 20").show()