package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.Logging
import com.aravind.oss.SOApp.spark
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object WordCountDFApp extends App with Logging {
  logInfo("WordCount with Dataframe API")

  val spark = SparkSession.builder()
    .appName("WordCountDFApp")
    .master("local[*]").getOrCreate()

  val linesDf: DataFrame = spark.read
    .textFile("src/main/resources/wordcount/test.txt")
    .toDF("line") //Dataset[Row]
  linesDf.show(5)

  val whitespaceRegex = "[\\s]"

  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val wordsDf = linesDf
    .select($"line", explode(split($"line", whitespaceRegex)).as("word"))
  wordsDf.show()

  wordsDf.filter($"word" =!= "")
    .groupBy(lower($"word"))
    .count()
    .show()
}