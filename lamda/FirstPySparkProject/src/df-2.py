import os
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark import SQLContext
#conf=SparkConf.setAppName("appName").setMaster("local")
sc=SparkContext.getOrCreate()
SqlContext=SQLContext(sc)
d=[{'combined_s':['1113','2223'],'cm15':'11122235555','age':58},{'combined_s':['111','222'],'cm15':'111222333444','age':28}]
df=SqlContext.createDataFrame(d)
print(df.show())