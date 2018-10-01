import csv

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark import SparkConf, SparkContext
import os

os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"

sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
sqlContext = SQLContext(sc)


df_ratings= sqlContext.read.\
    format('com.databricks.spark.csv').\
    options(header='true', inferschema='true',delimiter=',').\
    load("C:/Users/KiwiTech/Downloads/ratings_small.csv")


rating_count=df_ratings.groupBy('MovieID').count().drop_duplicates(['MovieID']).sort('count',ascending=False)

sorted=df_ratings.sort('rating',ascending=False)

rating_count.registerTempTable('rate')
sorted.registerTempTable('sorted')

df_result=sqlContext.sql('SELECT*FROM sorted  INNER JOIN rate on sorted.movieId=rate.movieId ')

df_result.drop_duplicates(['MovieID']).sort('count',ascending=False).show()



#movie_1= df_ratings.groupBy('MovieID').count().distinct().sort('count',ascending=False).SHOW()

#df_movies.join(movie_1,df_movies.MovieID == movie_1.MovieID,'inner').show()

#df_ratings.join(movie_1,df_ratings.MovieID == movie_1.MovieID,'inner').show()