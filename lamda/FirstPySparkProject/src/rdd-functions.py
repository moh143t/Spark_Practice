from __future__ import print_function
from pyspark import SparkConf, SparkContext

 

sparkConf = SparkConf().setAppName("rdd-function")
sc = SparkContext(conf = sparkConf)

rdd=sc.parallelize([1, 2,3,4])

print(rdd.map(lambda x: (x, 1)).collect())

# print(rdd.getNumPartitions())
# rdd=rdd.repartition(2)
# print(rdd.getNumPartitions())
# print(rdd.collect())



rdd1 = sc.parallelize([1, 2,3,4])
rdd2 = sc.parallelize([5,6])
rdd1.cartesian(rdd2).repartition(10).saveAsTextFile("rdd-functions_11")
print(rdd3.getNumPartitions())



rdd3.coalesce(2).saveAsTextFile("rdd-functions.txt")
x = sc.parallelize([("a", 1), ("b", 4),("a",3)])
y = sc.parallelize([("a", 2),("c",17)])

 #---------------------------------------------------  
for a,b in x.cogroup(y).collect():
    print(a)
    print(list(b[0]),list(b[1]))
#-----------------------------------------
def f(t):
    print('key->'+t[0])
    print("------------------------",list(t[1][0]))
    print("------------------------>>>",list(t[1][1]))    
x.cogroup(y).foreach(f)

#-------------------
#m = sc.parallelize([('name','Mayank'), ('age','25')]).collectAsMap()
#print(m)
# print(type(m))
#------------------
#rdd=sc.parallelize([2, 3, 4])
# print(rdd.count())
#------------------
rdd = sc.parallelize([("a", 1), ("b", 5), ("a", 4)])
print(rdd.countByKey())
rdd = sc.parallelize(["a","b","a","b","c",1,1])
print(rdd.countByValue())

# new_rdd=sc.parallelize([1, 1, 2, 3,"hello","hello"]).distinct()
# l=new_rdd.take(4)
# print(l)

#reduce
#rdd=sc.parallelize([1,2,3,4,5,6])
# sum=rdd.reduce(lambda x,y:x+y)
# print(sum)
#print(sc.parallelize([1.0, 2.0, 3.0]).sum())

#takeordered
#top
#print(sc.parallelize([10, 4, 2, 12, 3]).top(1))
#print(sc.parallelize([10, 4, 2, 12, 3]).top(3))
#print(sc.parallelize([10, 4, 2, 12, 3]).top(3,key=lambda x:-x))
#print(sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6))
#print(sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7], 2).takeOrdered(6, key=lambda x: str(x)))
#print(sc.parallelize([2, 3, 4]).first())
#print(sc.parallelize([2, 3, 4]).flatMap(lambda x: [x,x,x,x]).collect())
x = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
def f(x): return x
print(x.flatMapValues(f).collect())

def f(iterator):
    print('---------')
    for x in iterator:
        print(x)
sc.parallelize([1, 2, 3, 4, 5],2).foreachPartition(f)

#fullouterjoin
x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2), ("c", 8)])
print(x.fullOuterJoin(y).collect())
#glom

# rdd = sc.parallelize([1, 2, 3, 4], 2)
rdd.glom().foreach(lambda x: print(x))

# rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
# result = rdd.groupBy(lambda x: x % 2).foreach(lambda x:print(list(x[1])))
#l=rdd.groupBy(lambda x: 'V' if x in 'aeiou' else 'C').collect()
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2)])
print(rdd.groupByKey().collect())

# x = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"])])
# def f(x): return list(x)
# print(x.mapValues(f).collect())

print(sc.parallelize([("a", 1), ("b", 1), ("a", 1)]).reduceByKey(lambda x,y:x+y).take(10))

print(sc.parallelize([1, 1, 2, 3]).union(sc.parallelize([1, 1, 2, 4])).collect())
x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 3)])
y = sc.parallelize([("a", 3), ("c", None)])
print(x.subtract(y).collect())

# x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 2)])
# y = sc.parallelize([("a", 3), ("c", None)])
# print(x.subtractByKey(y).collect())

# data=[('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
# print(sc.parallelize(data).sortByKey().first())
data=[('hello', 1), ('boy', 2), ('100', 3), ('def', 4), ('2000', 5)]
#sc.parallelize(data).sortByKey(lambda x:x[1]).first()
#print(sc.parallelize(data).sortByKey(False, 10).getNumPartitions())
# data=[('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5),('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)]
# print(sc.parallelize(data).sortByKey(False, 3, keyfunc=lambda k: k.lower()).collect())
# print(sc.parallelize(data).sortBy(lambda x: x[0]).collect())

# print(sc.parallelize([(1, 2), (3, 4)]).values().collect())
#print(sc.parallelize([(1, 2), (3, 4)]).keys().collect())
# print(sc.parallelize([]).isEmpty())
# x = sc.parallelize([("a", 1), ("b", 4)])
# y = sc.parallelize([("a", 2), ("a", 3)])
# print(x.join(y).collect())

# x = sc.parallelize([("a", 1), ("b", 4)])
# y = sc.parallelize([("a", 2)])
# print(x.leftOuterJoin(y).collect())
# print(y.leftOuterJoin(x).collect())





