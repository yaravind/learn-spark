package com.aravind.oss.eg.spark.wordcount

import com.aravind.oss.SparkSessionTestWrapper
import com.aravind.oss.eg.spark.wordcount.WordCountDSApp.{Line, LineAndWord}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Encoders
import org.scalatest.FlatSpec

/**
 * Better than using spark-testingbase library:
 * 1. Faster
 * 2. Shows differences clearly when test fails
 * 3. Convinience flag to ignore the order of data in DataFrame
 */
class WordCountDSAppTestSpec extends FlatSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  "toWords" should "split the file into words" in {
    val sourceDf = Seq(
      ("one"),
      ("two"),
      (""),
      ("three Three")
    ).toDF("line").as[Line]

    val expectedDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("three Three", "three"),
      ("three Three", "Three"),
      ("", "")
    ).toDF("line", "word").as[LineAndWord]

    val actualDF = WordCountDSApp.toWords(sourceDf)

    assertSmallDatasetEquality(actualDF, expectedDF, orderedComparison = false)
  }

  "countWords" should "return count of each word" in {

    val wordsDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("three Three", "three"),
      ("three Three", "Three"),
      ("", "")
    ).toDF("line", "word").as[LineAndWord]

    val tupleEncoder = Encoders.tuple(Encoders.STRING, Encoders.LONG)
    val expectedDF = Seq(
      ("one", 1L),
      ("two", 1L),
      ("three", 2L)
    ).toDF("value", "count(1)").as[(String, Long)]

    val actualDF = WordCountDSApp.countWords(wordsDF)

    assertSmallDatasetEquality(actualDF, expectedDF, orderedComparison = false)
  }
}
