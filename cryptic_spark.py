from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('myTest').setMaster('yarn')
sc = SparkContext(conf=conf)

# create RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20.0), ("Denny", 31.0), ("Jules", 30.0),
    ("TD", 35.0), ("Brooke", 25.0)])

#use map and reduceByKey with their lambda expressions to aggregate
#and then to compute average
agesRDD = dataRDD\
  .map(lambda x: (x[0], (x[1], 1)))\
  .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
  .map(lambda x: (x[0], x[1][0]/x[1][1]))

print agesRDD.collect()
