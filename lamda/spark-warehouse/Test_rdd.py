import csv

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"
Exam_3='C:\Users\KiwiTech\Downloads\UIDAI-ENR-DETAIL-20170308.csv'

sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
sqlContext = SQLContext(sc)

rdd=sc.textFile(Exam_3)

data=rdd.map(lambda x:x.split(",")).filter(lambda x:x if x[6]=='F' else "").filter(lambda x:x if x[3] in ('Varanasi') else '')


print data.collect()