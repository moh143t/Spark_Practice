
from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"

sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
sqlContext = SQLContext(sc)

df_CRIME= sqlContext.read.\
    format('com.databricks.spark.csv').\
    options(header='true', inferschema='true',delimiter=',').\
    load("C:\Users\KiwiTech\Downloads\crime_data.csv")

df_c=df_CRIME.drop('long','lat')

#top_location=df_c.groupBy('location').count().sort('count',ascending=False).withColumnRenamed("count", "location_count")

#top_catogry=df_c.groupBy('category').count().sort('count',ascending=False).withColumnRenamed("count", "catogery_count").show(1)

df_c.registerTempTable('crime')

bulagry_crime=sqlContext.sql("SELECT *FROM crime WHERE category IN ('Violence and sexual offences')")

bulagry_crime.groupBy('location').count().sort('count',ascending=False).withColumnRenamed("count", "location_count").show()
