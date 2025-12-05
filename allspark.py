from pyspark.sql import SparkSession

#create sparksession
spark=SparkSession.builder.appName("all spark").getOrCreate()
print(spark.version)

#2. Create RDD from list
data=[2,4,1,5,6,3]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

rdd2=rdd.map(lambda x:x*2)
# print(rdd2.collect())

rdd3=rdd.map(lambda x: x**3)
# print(rdd3.collect())

rdd4=rdd2.map(lambda x: x*3)
# print(rdd4.collect())

rdd5 =rdd3.filter(lambda x:x>50)
print(rdd5.collect())

print(rdd.take(3))  #---take() is used for 1st n element
print(rdd.first())  #---return only first element
print(rdd.count())  #---return number of elements
print(rdd.top(3))