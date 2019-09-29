from pyspark import SparkConf, SparkContext
import os
sparkConf = SparkConf().setAppName("Myfirstapp")
sc = SparkContext(conf = sparkConf)
currentPath=os.getcwd()

#print(currentPath)
f1="/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/input_files/routes.txt"
f2="/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/input_files/cities.txt"
#print(sc.textFile(f1).first())

cities_rdd=sc.textFile(f2).map(lambda x: (x.split(",")[0],x.split(",")[1]))

rdd2=sc.textFile(f1).map(lambda x:(x.split(",")[1],(x.split(",")[2],x.split(",")[0]))).join(cities_rdd).map(lambda x:(x[1][0][0],(x[1][0][1],x[1][1]))).join(cities_rdd).map(lambda x:(x[1][0][0],",",x[1][0][1],",",x[1][1])).coalesce(2).saveAsTextFile("/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/output_files/")
#rdd2.map(lambda x:(x[1][0][0],(x[1][0][1],x[1][1]))).join(cities_rdd).map(lambda x:x[1][0][0]+","+x[1][0][1]+","+x[1][1]).coalesce(2).saveAsTextFile("/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/output_files/")


#exit(0)
#alternate

cities_map=cities_rdd.collectAsMap()
b_c_cities=sc.broadcast(cities_map)
sc.textFile(f1).map(lambda x:x.split(",")[0]+b_c_cities.value[x.split(",")[1]]+b_c_cities.value[x.split(",")[2]]).coalesce(1).saveAsTextFile("/home/harsh/mapping_minds_training/spark/py_spark_work/FirstPySparkProject/output_files")