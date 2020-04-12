package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DatasetComparer
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
}
