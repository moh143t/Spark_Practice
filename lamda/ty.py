import os



from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext


# Path for spark source folder
os.environ['SPARK_HOME']="C:\Users\KiwiTech\Downloads\spark-1.6.3-bin-hadoop2.6\spark-1.6.3-bin-hadoop2.6"
Exam_3='C:\Users\KiwiTech\Downloads\googleplaystore.csv'

sc = SparkContext(conf=SparkConf().setAppName('myapp').setMaster('local'))
lines=sc.textFile(Exam_3)
rateline1=lines.filter(lambda x:'4.1' in  x)
rateline4=lines.filter(lambda x:'4.4' in  x)

result=rateline1.union(rateline4).take(3)
print result
