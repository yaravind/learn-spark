package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCountRddAppV2 extends App with Logging {
  logInfo("WordCount with RDD API")

  val spark = SparkSession.builder()
    .appName("WordCountRddApp")
    .master("local[*]").getOrCreate()

  val lines: RDD[String] = spark.read
    .textFile("src/main/resources/wordcount/test.txt") //Dataset[String]
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
    //matches a space, a tab, a carriage return, a line feed, or a form feed
    val whitespaceRegex = "[\\s]"

    lines
      .map(line => line.split(whitespaceRegex)) //Array[Array[String]]
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
