from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('myTest').setMaster('yarn')
sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print "Hello from Dimitar's RDD test 01"
res=distData.reduce(lambda a, b: a + b)
print "Result from distData.reduce(..):"
print res
distFile = sc.textFile("textFile.txt")
res2=distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)
print "Result from distFile.map(..).reduce(..):"
print res2

