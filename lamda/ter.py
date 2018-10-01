from pyspark import SparkConf, SparkContext
import os

# Path for spark source folder
os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"
file='C:/Users/KiwiTech/Downloads/googlenew.csv'
sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
lines=sc.textFile(file)
"""split=lines.\
    map(lambda x:x.split(',')).\
    filter(lambda x:x if x[-1].lower() in ('red','black') else '').\
    groupByKey().\
    mapValues(list).\
    filter(lambda x:len(x[-1])==2).\
    collect()


split=lines.\
    map(lambda x:x.split(',')).\
    filter(lambda x:x if x[-1]<'1.1.0' else '').\
    filter(lambda x:x if x[2]=='4.4' else '').\
    collect()
"""

r={
  "brand": "Ford",
  "model": "Mustang",
  "year": 1964
}

rdd3=sc.parallelize(r)
print type(rdd3)


