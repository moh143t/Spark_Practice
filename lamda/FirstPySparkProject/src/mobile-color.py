'''
Created on Jul 4, 2018

@author: harsh
'''
from pyspark import SparkConf, SparkContext

 

sparkConf = SparkConf().setAppName("Myfirstapp")
sc = SparkContext(conf = sparkConf)
 
input_rdd = sc.textFile("/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/mobile-color-input.txt")
input_rdd.map(lambda record:(record[0],record[2:])).filter(lambda record:record[1].lower() in ('red','black')).groupByKey().filter(lambda record:len(record[1])==2).keys().collect()
