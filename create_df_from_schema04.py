from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

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

schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())])

#df = spark.createDataFrame(data=data, schema = columns)
df = spark.createDataFrame(data=data, schema=schema)
dfNestedColsRenamed=df.select(col("name").cast(schema2),\
    col("dob"),\
    col("gender"),\
    col("salary")) 

dfColRenamed = df.withColumnRenamed("dob", "DateOfBirth")

dfRenamedBySelect = df.withColumn("fname",col("name.firstname")) \
      .withColumn("mname",col("name.middlename")) \
      .withColumn("lname",col("name.lastname")) \
      .drop("name")

print "Hello from Dimitar's RDD create dataframe from schema test 03"
print "Printing schema of dataframe created from schema:"
df.printSchema()
print "Printing dataframe created from schema:"
df.show(truncate=False)

print "Printing schema of dataframe created from schema with nested columns renamed:"
dfNestedColsRenamed.printSchema()
print "Printing dataframe created from schema with nested columns renamed:"
dfNestedColsRenamed.show(truncate=False)

print "Printing schema of dataframe created from schema with column renamed:"
dfColRenamed.printSchema()
print "Printing dataframe created from schema with column renamed:"
dfColRenamed.show(truncate=False)

print "Printing schema of dataframe created from schema with columns renamed by select():"
dfRenamedBySelect.printSchema()
print "Printing dataframe created from schema with columns renamed by select():"
dfRenamedBySelect.show(truncate=False)
