from pyspark.sql import SparkSession

#conf = SparkConf().setAppName('rddCreateTest01').setMaster('yarn')
#osc = SparkContext(conf=conf)
spark = SparkSession.builder\
      .master("yarn")\
      .appName("SparkByExamples.com")\
      .getOrCreate()

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)
print "Hello from Dimitar's RDD create test 01"
rdd.saveAsSequenceFile("rddSequenceFile2")
res=sorted(spark.sparkContext.sequenceFile("rddSequenceFile2").collect())
print "Result from sorted(sc.sequenceFile..collect()):"
print res

