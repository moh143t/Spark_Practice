from pyspark import SparkConf, SQLContext, SparkContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import *


sparkConf = SparkConf().setAppName("DF-examples")
sc = SparkContext(conf = sparkConf)
sqlContext = SQLContext(sc)


l = [('paul', 20),('bob',21),('cat',10)]
df=sqlContext.createDataFrame(l)
#print(df)



df=sqlContext.createDataFrame(l, ['name', 'age'])
# print(df)

d = [{'name': 'paul', 'age': 10,'gender':'male'},{'name': 'alice', 'age': 30,'gender':None}]
print(sqlContext.createDataFrame(d).collect())
# 
rdd = sc.parallelize(l)
df = sqlContext.createDataFrame(rdd, ['name', 'age'])
# print(df.collect())
#df=sqlContext.createDataFrame(rdd)
# print(df.printSchema())
print(df.head(2))

sqlContext.registerDataFrameAsTable(df, "table1")
df2 = sqlContext.sql("SELECT name,age from table1 where name='bob'")
print(df2.collect())
print(sqlContext.tableNames())
sqlContext.dropTempTable("table1")
print(sqlContext.tableNames())


df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("/home/harsh/mapping_minds_training/spark/train_u6lujuX_CVtuZ9i.csv")
print(df.groupBy('Gender').agg({'ApplicantIncome': 'mean'}).show())
print(df.head(3))
print(df.printSchema())
print(df.columns)
df.cache()
print('count-------------------------->',df.count())

#print(df.describe().show())
#print('distinct count------------------------------>',df.distinct().count())
#df.unpersist()


#df1=df.drop('Gender')
#print(df1.show())

#print(df.agg({"CoapplicantIncome": "max"}).collect())
#df.cache()
#print(df.show())
#print("total count------------>",df.count())
#print("drop duplicates ----->",df.dropDuplicates().count())


df = sc.parallelize([Row(name="CAT", age=5),Row(name='Alice', age=80),Row(name='BOB', age=80)]).toDF()
df2 = sc.parallelize([Row(name="CAT", height=18),Row(name='Alice', height=28),Row(name='BOB', height=80)]).toDF()
df3 = sc.parallelize([Row(name="CAT", height=18,gender='Male'),Row(name=None,height=28,gender=None),Row(name='BOB', height=None,gender='Female')]).toDF()
# print(df.na.drop().show())
# #print(df.dtypes)
# print(df.filter("Gender = 'Male'").show())
#print(df.first())

#print(df.groupBy().avg().collect())
#print(df.groupBy('name').agg({'age': 'mean'}).show())
#join

#df1=df.join(df2, df.name == df2.name,'inner').select(df.name, df2.height)
#print(df1.show())


#print(df.limit(1).show())
print(df.repartition(10).rdd.getNumPartitions())

#data = df.unionAll(df)
#print(data.show())
#data=df.select(df.name, (df.age + 10).alias('age1'))
#print(data.toJSON().collect())
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("")

