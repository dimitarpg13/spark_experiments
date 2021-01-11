package main.scala.chapter3 

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object DfFromBlogJson extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("yarn")
    .appName("DfFromBlogJson")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
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
  println("Show the contents of the dataframe")
  blogsDF.show(false)

  println("Show blogsDF schema")
  blogsDF.printSchema
  println(blogsDF.schema)

  println("Show blogsDF columns")
  println(blogsDF.columns)
  println(blogsDF.col("Id"))

  println("Use select + expr to compute a value from column data")
  val hitsPlusTwoExpr = blogsDF.select(expr("Hits * 2"))
  hitsPlusTwoExpr.show(2)
  
  println("Use select + col to compute a value from column data")
  val hitsPlusTwoExprWithCol = blogsDF.select(col("Hits") * 2)
  hitsPlusTwoExprWithCol.show(4)

  println("""Use an expression to compute big hitters for blogs. 
    This adds a new column, Bit Hitters, based on the conditional expression""")
  val bigHittersExpr = blogsDF.withColumn("Big Hitters", (expr("Hits > 10000")))
  bigHittersExpr.show()

  println("""Concatenate three columns, create a new column, and show the newly
    created concatenated column""")
  val threeColConcat = blogsDF
    .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
    .select(col("AuthorsId"))
  threeColConcat.show(4)

  println("""These statements return the same value, showing that expr
    is the same as a col method call""")
  val HitsVar1 = blogsDF.select(expr("Hits"))
  val HitsVar2 = blogsDF.select(col("Hits"))
  val HitsVar3 = blogsDF.select("Hits")
  println("""blogsDF.select(expr("Hits")).show(2)""")
  HitsVar1.show(2)
  
  println("""blogsDF.select(col("Hits")).show(2)""")
  HitsVar2.show(2)

  println("""blogsDF.select("Hits").show(2)""")
  HitsVar3.show(2)

  println("""Sort by column "Id" in descending order""")
  val sortByColIdVar1 = blogsDF.sort(col("Id").desc)
  val sortByColIdVar2 = blogsDF.sort($"Id".desc)
  println("""blogsDF.sort(col("Id").desc).show()""")
  sortByColIdVar1.show()
  println("""blogsDF.sort($"Id".desc).show()""")
  sortByColIdVar2.show()

}
