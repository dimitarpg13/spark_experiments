package main.scala.chapter3 

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession


object DfFromBlogJson extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("yarn")
    .appName("DfFromBlogJson")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")

  val jsonFile = "hdfs:///user/dimitar_pg13/LearningSparkV2/chapter3/data/blogs.json" 
  println("spark read JSON Blog into RDD")

  // define our schema programmatically
  val schema = StructType(Array(StructField("Id", IntegerType, false),
    StructField("First", StringType, false),
    StructField("Last", StringType, false),
    StructField("Url", StringType, false),
    StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false),
    StructField("Campaigns", ArrayType(StringType), false)))

  // create a dataframe by reading from the JSON file
  // with predefined schema
  val blogsDF = spark.read.schema(schema).json(jsonFile)
  // Show the DataFrame schema as output
  blogsDF.show(false)
  println(blogsDF.printSchema)
  println(blogsDF.schema)
}
