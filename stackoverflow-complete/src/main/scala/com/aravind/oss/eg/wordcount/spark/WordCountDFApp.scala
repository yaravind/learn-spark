package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.Logging
import WordCountUtil._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object WordCountDFApp extends App with Logging {
  logInfo("WordCount with Dataframe API")

  val paths = getPaths(args)
  val cluster = getClusterCfg(args)

  if (paths.size > 1) {
    logInfo("More than one file to process")
  }
  logInfo("Path(s): " + paths)
  logInfo("Cluster: " + cluster)

  val spark = getSparkSession("WordCountDFApp", cluster)

  val linesDf: DataFrame = spark.read
    .textFile(paths: _*)
    .toDF("line") //Dataset[Row]
  logInfo("DataFrame before splitting line")
  linesDf.show(false)

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val wordsDf = linesDf
    .select($"line",
      explode(split($"line", WhitespaceRegex)).as("word"))
  logInfo("Inferred schema")
  wordsDf.printSchema()
  logInfo("DataFrame after splitting the line into words")
  wordsDf.show(false)

  wordsDf.filter($"word" =!= "")
    .groupBy(lower($"word"))
    .count()
    //.explain()
    .show(false)
}