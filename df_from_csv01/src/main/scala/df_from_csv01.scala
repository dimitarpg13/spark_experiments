package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ReadMultipleCSVFiles extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("yarn")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println("spark read csv files from a directory into RDD")
  val rddFromFile = spark.sparkContext.textFile("/user/dimitar_pg13/vega-datasets/data/la-riots.csv")
  println(rddFromFile.getClass)

  val rdd = rddFromFile.map(f=>{
    f.split(",")
  })

  println("Iterate RDD")
  rdd.foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
  println(rdd)
}
