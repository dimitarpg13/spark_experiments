from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#conf = SparkConf().setAppName('rddCreateTest01').setMaster('yarn')
#osc = SparkContext(conf=conf)
spark = SparkSession.builder\
      .master("yarn")\
      .appName("SparkByExamples.com")\
      .getOrCreate()

data = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

#columns = ["firstname","middlename","lastname","dob","gender","salary"]
schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
        ])),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
    ])


#df = spark.createDataFrame(data=data, schema = columns)
df = spark.createDataFrame(data=data, schema=schema)
dfColRenamed = df.withColumnRenamed("dob", "DateOfBirth")

print "Hello from Dimitar's RDD create dataframe from schema test 01"
print "Printing schema of dataframe created from schema:"
print df.printSchema()
print "Printing dataframe created from schema:"
print df.show(truncate=False)

print "Printing schema of dataframe created from schema with column renamed:"
print dfColRenamed.printSchema()
print "Printing dataframe created from schema with column renamed:"
print dfColRenamed.show(truncate=False)
