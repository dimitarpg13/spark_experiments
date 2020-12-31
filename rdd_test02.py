from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('myTest').setMaster('yarn')
sc = SparkContext(conf=conf)

print "Hello from Dimitar's RDD test 02"
rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd.saveAsSequenceFile("rddSequenceFile")
res=sorted(sc.sequenceFile("rddSequenceFile").collect())
print "Result from sorted(sc.sequenceFile..collect()):"
print res

