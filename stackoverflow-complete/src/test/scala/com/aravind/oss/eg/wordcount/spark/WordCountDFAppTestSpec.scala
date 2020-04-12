package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.SparkSessionTestWrapper
import org.scalatest.FlatSpec
import com.github.mrpowers.spark.fast.tests.DatasetComparer

/**
 * Better than using spark-testingbase library:
 * 1. Faster
 * 2. Shows differences clearly when test fails
 */
class WordCountDFAppTestSpec extends FlatSpec with SparkSessionTestWrapper with DatasetComparer {

  val input: Seq[String] = Seq(
    ("one"),
    ("two"),
    (""),
    ("three Three")
  )

  import spark.implicits._

  "toWords" should "split the file into words" in {
    val sourceDf = input.toDF("line")

    val expectedDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("three Three", "three"),
      ("three Three", "Three"),
      ("", "")
    ).toDF("line", "word")

    val actualDF = WordCountDFApp.toWords(sourceDf)

    assertSmallDatasetEquality(actualDF, expectedDF, orderedComparison = false)
  }
}
