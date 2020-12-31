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
#df = spark.createDataFrame(data=data, schema = columns)
dfFromData2 = spark.createDataFrame(data).toDF(*columns)

print "Hello from Dimitar's RDD create dataframe test 02"
print "Printing dataframe created from data:"
print dfFromData2.show() 

