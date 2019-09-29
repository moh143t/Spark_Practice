'''
Created on Jul 24, 2017

@author: harsh
'''

from pyspark import SparkConf, SparkContext

 

sparkConf = SparkConf().setAppName("Myfirstapp")
sc = SparkContext(conf = sparkConf)
 
textFile = sc.textFile("/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/name.txt")
wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
#print(wordCounts.toDebugString())
for wc in wordCounts.collect(): 
    print wc
sc.stop()




