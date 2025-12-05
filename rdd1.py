
from pyspark.sql import SparkSession
spark=SparkSession.builder\
.appName("MySQL_Read") \
      .getOrCreate()  
sc=spark.sparkContext

#----RDD is like tuple it is immutable
#-------RDD using List-----

#1.create RDD
# List=[10,20,10,30,40,50,60]
# rdd=sc.parallelize(List)

#2.Display RDD
# print(rdd.collect())

#3.Find distinct from RDD
# print(rdd.distinct().collect())

#4.Lambda functions in rdd with map()
# rdd_add=rdd.map(lambda x:x+5)   #addition
# print(rdd_add.collect())

# rdd_sq=rdd.map(lambda x:x**2)   #addition
# print(rdd_sq.collect())

# rdd_mul=rdd.map(lambda x:x*5)   #multiplication
# print(rdd_mul.collect())

#5.Lambda functions in rdd with filter()
# rdd_filter=rdd.filter(lambda x: x>30)
# print(rdd_filter.collect())

# rdd_filter=rdd.filter(lambda x:x==20) 
# print(rdd_filter.collect())

# dict_rdd=(["omkar","satara","ashok","Beed"])
# dd=sc.parallelize(dict_rdd)
# # print(dd.collect())
# rdd_flat=dd.flatMap(lambda line: line.split(" "))
# print(rdd_flat.collect())


# 6.two list merging
# List=[10,24,25,54,65,85,24,45]
# List2=[89,54,25,47,65,64,66,48]
# rdd=sc.parallelize(List)
# rdd1=sc.parallelize(List2)
# rdd_union=rdd.union(rdd1)
# print(rdd_union.collect())

#7.create dataframe from dict
# dict_rdd=dict_RDD =[("omkar","satara"),("ashok","Beed")]
# expo=spark.createDataFrame(dict_rdd,['name','address'])
# expo.show() 
 
#-for print data frame we use show()
#-for print RDD we use print()

data = [("a", 1), ("b", 2), ("a", 3),("a",10)]
rdd_kv = sc.parallelize(data)
#8.reduce by
# rdd_reduced = rdd_kv.reduceByKey(lambda a, b: a + b)
# print(rdd_reduced.collect()) # [('a', 4), ('b', 14)]

#9.groupby
# rdd_group = rdd_kv.groupByKey().mapValues(list)
# print(rdd_group.collect()) # [('a', [1, 3]), ('b', [2])]

#10.sortby
# rdd_sorted = sc.parallelize([('b', 2), ('a', 3), ('c', 1)]).sortByKey()
# print(rdd_sorted.collect()) # [('a', 3), ('b', 2), ('c', 1)]

#----RDD actions
# Create RDD
List = [10, 20, 30, 40, 50]
rdd = sc.parallelize(List)

# Count elements
# print("Count:", rdd.count())

# # Get first element
# print("First:", rdd.first())

# Get top 3 elements
print("Top 3:", rdd.top(3))

# Get max elements
print("max:", rdd.max())

# Get min elements
print("min:", rdd.min())

# Sum and average
# print("Sum:", rdd.sum())
# print("Average:", rdd.mean())

 
 
 
 
