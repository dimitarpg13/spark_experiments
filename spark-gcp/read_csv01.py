from pyspark.sql import SparkSession

#conf = SparkConf().setAppName('rddCreateTest01').setMaster('yarn')
#osc = SparkContext(conf=conf)
spark = SparkSession.builder\
      .master("yarn")\
      .appName("SparkByExamples.com")\
      .getOrCreate()

df = spark.read.csv("vega-datasets/data/zipcodes.csv")

print "Hello from Dimitar's RDD create dataframe test 01"
print "Printing schema for dataframe created from vega datasets zipcode:"
print df.printSchema() 

