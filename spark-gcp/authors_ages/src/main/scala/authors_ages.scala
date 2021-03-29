package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AuthorsAges {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("AuthorsAgesScala")
      .getOrCreate()
    // create a dataframe of names and ages
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
    // group the same names together, aggregate their ages, and compute 
    // an average
    val avgDF = dataDF.groupBy("name").agg(avg("age"))
    // show the result of the final execution
    avgDF.show()

    spark.stop()
  }
}
