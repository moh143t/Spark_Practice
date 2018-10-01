import csv

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"
Exam_3='C:\Users\KiwiTech\Downloads\UIDAI-ENR-DETAIL-20170308.csv'

sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
sqlContext = SQLContext(sc)

df = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true').load(Exam_3)



df.registerTempTable('ram')
data=sqlContext.sql("SELECT * FROM ram WHERE Gender='F' AND District IN ('Varanasi')")

print data.count()
