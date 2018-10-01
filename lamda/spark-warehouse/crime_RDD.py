import csv

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"

sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
sqlContext = SQLContext(sc)

data=sc.textFile('C:\Users\KiwiTech\Downloads\crime_data.csv')


result=data.map(lambda x:x.split(",")).filter(lambda x:x if x[4] in ('Vehicle crime') else "").map(lambda x:(x[3],1)    )


print result.take(5)