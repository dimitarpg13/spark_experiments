from pyspark.sql import SparkSession, Row

row = Row("James",40)
print(row[0]+","+str(row[1]))
row2=Row(name="Alice", age=11)
print(row2.name)

Person = Row("name", "age")
p1 = Person("James", 40)
p2 = Person("Alice", 35)
print(p1.name+","+p2.name)

#PySpark example
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
        Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
        Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]

#RDD Example 1
rdd = spark.sparkContext.parallelize(data)
collData = rdd.collect()
print(collData)
for person in collData:
    print(person.name + "," + str(person.lang))

#RDD Example 2
Person=Row("name","lang","state")
data = [Person("James,,Smith",["Java","Scala","C++"],"CA"),
        Person("Michael,Rose,",["Spark","Java","C++"],"NJ"),
        Person("Robert,,Williams",["CSharp","VB"],"NV")]
rdd = spark.sparkContext.parallelize(data)
collData = rdd.collect()
print(collData)
for person in collData:
    print(person.name + "," + str(person.lang))

#DataFrame Example 1
columns = ["name","languagesAtSchool","currentState"]
df = spark.createDataFrame(data)
df.printSchema()
df.show()

collData = df.collect()
print(collData)
for row in collData:
    print(row.name + "," + str(row.lang))

#DataFrame Example 2
columns = ["name","languageAtSchool","currentState"]
df = spark.createDataFrame(data).toDF(*columns)
df.printSchema()
