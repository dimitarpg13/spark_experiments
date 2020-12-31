from pyspark.sql import SparkSession

#conf = SparkConf().setAppName('rddCreateTest01').setMaster('yarn')
#osc = SparkContext(conf=conf)
spark = SparkSession.builder\
      .master("yarn")\
      .appName("SparkByExamples.com")\
      .getOrCreate()

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
rdd = spark.sparkContext.parallelize(data)
dfFromRDD1 = rdd.toDF()
print "Hello from Dimitar's RDD create dataframe from rddtest 01"
print "Printing schema for the df created from rdd:"
print dfFromRDD1.printSchema()
print "Printing dataframe created from rdd:"
print dfFromRDD1.show() 


