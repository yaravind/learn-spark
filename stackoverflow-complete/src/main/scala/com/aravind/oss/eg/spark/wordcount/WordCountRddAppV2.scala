package com.aravind.oss.eg.spark.wordcount

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import com.aravind.oss.eg.spark.wordcount.WordCountUtil.{WhitespaceRegex, getPaths}
import org.apache.spark.rdd.RDD

object WordCountRddAppV2 extends App with Logging {
  logInfo("WordCount with RDD API")

  val paths = getPaths(args)
  val cluster = getClusterCfg(args)

  if (paths.size > 1) {
    logInfo("More than one file to process")
  }
  logInfo("Path(s): " + paths)
  logInfo("Cluster: " + cluster)

  val spark = getSparkSession("WordCountRddAppV2", cluster)

  //_* creates a var arg from Seq. Refer: https://scastie.scala-lang.org/stefanobaghino/imccZJl0QDCQupLtrPm2Og
  val lines: RDD[String] = spark.read
    .textFile(paths: _*) //Dataset[String]
    .rdd

  val wordCounts = countWords(lines)
  wordCounts.foreach(println)
  val count = wordCounts.count
  logInfo("Word count = " + count)

  def countWords(lines: RDD[String]): RDD[(String, Int)] = {
    val words = toWords(lines)
    countByWord(words)
  }

  def toWords(lines: RDD[String]): RDD[String] = {
    logInfo("splitting into words")

    lines
      .map(line => line.split(WhitespaceRegex)) //Array[Array[String]]
      .flatMap(wordArray => wordArray)
  }

  def countByWord(words: RDD[String]): RDD[(String, Int)] = {
    words
      .filter(word => !word.isEmpty) //filter empty strings
      .map(word => word.toLowerCase) //put in lower case
      .map(word => (word, 1))
      .reduceByKey { case (x, y) => x + y }
  }
}
