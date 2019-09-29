#from pyspark import SparkContext

from pyspark import SparkContext, SparkConf

from pyspark.sql import SQLContext

from pyspark import sql

 

#Create spark Context with desired configuration

conf = SparkConf().setAppName("resume_analysis")

sc = SparkContext(conf=conf)

sqlContext = sql.SQLContext(sc)

 

#Read folder that contain multiple resumes and convert in data frame

df=sc.wholeTextFiles('/home/harsh/git/resumes').toDF()

from pyspark.sql.types import StringType

from pyspark.sql.types import IntegerType

from pyspark.sql.functions import udf

import json

 

 

#Predefined Skills which needs to be fecthed from resume content

skills=['spark','python','hive']

#Since this list is supposed to read by each tasks so make it a broadcast variable

b_skills=sc.broadcast(skills)

 

#filter the skills from the content

def getSkills(data):

        d={}

        l=filter(lambda x: x.lower() in b_skills.value ,data.split())

        for k in l:
            k=k.lower()
            d[k]=d.get(k,0)+1
            pass

        return json.dumps(d)

                               

s=udf(lambda x:getSkills(x),StringType())

df=df.withColumn('skills',s(df._2)).drop(df._2)


#get file name from the absolute path of file

getFileName=udf(lambda x : x.split('/')[-1],StringType())

df=df.withColumn('fileName',getFileName(df._1)).drop(df._1)

print df.collect() 

#define the weight table and make it broadcast variable

weightage={'spark':10,'python':2,'hive':4}

bweightage=sc.broadcast(weightage)

#create method which computes the weightage for each skills

#add_weightage=udf(lambda x:json.dumps({k:bweightage.value.get(k,0)+v  for k,v in json.loads(x).items()}))

add_weightage=udf(lambda x:json.dumps({k:bweightage.value.get(k,0)*v  for k,v in json.loads(x).items()}))

df=df.withColumn('skills',add_weightage(df.skills))

print df.collect()
#add all the computed weightage for each resume

get_total=udf(lambda x:sum(list(json.loads(x).values())),IntegerType())

df=df.withColumn('skills',get_total(df.skills))

#save the data in HDFS in RDD format

df.rdd.map(lambda x:str(x[0])+","+str(x[1])).saveAsTextFile('resume_analysis')

sc.stop()