package com.aravind.oss.eg.wordcount.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FlatSpec

class WordCountDFAppTestSpec2 extends FlatSpec with DataFrameSuiteBase {

  val input: Seq[String] = Seq(
    ("one"),
    ("two"),
    (""),
    ("three Three")
  )

  "toWords" should "split the file into words" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val sourceDf = input.toDF("line")
    // sourceDf.show(false)

    val expectedDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("", ""),
      ("three Three", "three"),
      ("three Three", "Three")
    ).toDF("line", "word")
    // expectedDF.show(false)

    val actualDF = WordCountDFApp.toWords(sourceDf)
    // actualDF.show(false)

    assertDataFrameEquals(actualDF, expectedDF)
  }

  "countWords" should "return count of each word" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val wordsDF = Seq(
      ("one", "one"),
      ("two", "two"),
      ("three Three", "three"),
      ("three Three", "Three"),
      ("", "")
    ).toDF("line", "word")

    val expectedDF = Seq(
      ("one", 1L),
      ("two", 1L),
      ("three", 2L)
    ).toDF("lower(word)", "count")

    val actualDF = WordCountDFApp.countWords(wordsDF)

    assertDataFrameNoOrderEquals(actualDF, expectedDF)
  }
}
