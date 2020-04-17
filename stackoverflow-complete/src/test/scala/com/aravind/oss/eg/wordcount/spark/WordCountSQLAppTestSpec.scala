package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.FlatSpec

/**
 * Better than using spark-testingbase library:
 * 1. Faster
 * 2. Shows differences clearly when test fails
 * 3. Convinience flag to ignore the order of data in DataFrame
 */
class WordCountSQLAppTestSpec extends FlatSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  "splitToWords" should "split the file into words" in {
    val sourceDf = Seq(
      "one",
      "two",
      "",
      "three Three"
    ).toDF("line")
    sourceDf.createOrReplaceTempView("lines_tbl")

    val expectedDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("three Three", "three"),
      ("three Three", "Three"),
      ("", "")
    ).toDF("line", "word")

    WordCountSQLApp.splitToWords(spark)
    val actualDF = spark.table("words_tbl")

    assertSmallDatasetEquality(actualDF, expectedDF, orderedComparison = false)
  }

  "countWords" should "return count of each word" in {

    val wordsDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("three Three", "three"),
      ("three Three", "Three"),
      ("", "")
    ).toDF("line", "word")
    wordsDF.createOrReplaceTempView("words_tbl")

    val expectedDF = Seq(
      ("one", 1L),
      ("two", 1L),
      ("three", 2L)
    ).toDF("lower(word)", "count")

    val actualDF = WordCountSQLApp.countWords(spark)

    assertSmallDatasetEquality(actualDF, expectedDF, orderedComparison = false)
  }
}
