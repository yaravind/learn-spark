package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.Logging
import org.apache.spark.rdd.RDD
import WordCountUtil._

object WordCountRddApp extends App with Logging {
  logInfo("WordCount with RDD API")

  val paths = getPaths(args)
  val cluster = getClusterCfg(args)

  if (paths.size > 1) {
    logInfo("More than one file to process")
  }
  logInfo("Path(s): " + paths)
  logInfo("Cluster: " + cluster)

  val spark = getSparkSession("WordCountRddApp", cluster)

  val lines: RDD[String] = spark.read
    .textFile(paths: _*) //Dataset[String]
    .rdd
  logInfo("Line count: " + lines.count())

  val words: RDD[String] = lines
    .map(line => line.split(WhitespaceRegex)) //Array[Array[String]]
    .flatMap(wordArray => wordArray)

  logInfo("Version 1: Layman's word count")
  var wordsV1 = words
    .map(word => (word, 1))
    .reduceByKey { case (x, y) => x + y }

  wordsV1.foreach(println)
  var count = wordsV1.count
  logInfo("Version 1: Word count = " + count)

  logInfo("Version 2: V1 + Filter empty strings")
  val wordsV2 = words
    .filter(word => !word.isEmpty) //filter empty strings
    .map(word => (word, 1))
    .reduceByKey { case (x, y) => x + y }
  wordsV2.foreach(println)
  count = wordsV2.count
  logInfo("Version 2: Word count = " + count)

  logInfo("Version 3: V2 + Ignore case")
  val wordsV3 = words
    .filter(word => !word.isEmpty) //filter empty strings
    .map(word => word.toLowerCase) //put in lower case
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  wordsV3.foreach(println)
  count = wordsV3.count
  logInfo("Version 3: Word count = " + count)
  //words.foreach(w => logInfo("[" + w + "]"))
}
