from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
from pyspark.sql.functions import col

#conf = SparkConf().setAppName('rddCreateTest01').setMaster('yarn')
#osc = SparkContext(conf=conf)
spark = SparkSession.builder\
      .master("yarn")\
      .appName("SparkByExamples.com")\
      .getOrCreate()

arrayStructureData = [
        (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
        (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
        (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
        (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
        (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
        (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
        ]
        
arrayStructureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('languages', ArrayType(StringType()), True),
         StructField('state', StringType(), True),
         StructField('gender', StringType(), True)
         ])


df = spark.createDataFrame(data = arrayStructureData, schema = arrayStructureSchema)

print "Hello from Dimitar's RDD create dataframe with filter 01"
print "Printing schema of the dataframe created from data.."
df.printSchema()
print "Printing dataframe created from data.."
df.show(truncate=False) 

print "Printing dataframe created from data with filter applied.."
df.filter(df.state == "OH").show(truncate=False)

print "Printing dataframe created from data with col-instance filter applied.."
df.filter(col("state") == "OH").show(truncate=False)

print "Printing dataframe created from data with SQL expression filter applied.."
df.filter("gender  == 'M'").show(truncate=False)

print "Printing dataframe create from data with multiple conditions filter.."
df.filter( (df.state  == "OH") & (df.gender  == "M") ).show(truncate=False)  
