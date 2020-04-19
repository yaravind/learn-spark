package com.aravind.oss.eg.spark.wordcount

import com.aravind.oss.BaseSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class WordCountRddAppTestSpec extends BaseSpec with SharedSparkContext {
  val input: Array[String] = Array("one", "two", "three Three", "four          FOUR")

  "toWords" should "split the file into words" in {
    val lines: RDD[String] = sc.parallelize(input)
    val words = WordCountRddAppV2.toWords(lines)
    assert(words.count() == 15)
  }

  "countWords" should "return count of each word" in {
    val lines: RDD[String] = sc.parallelize(input)
    val wordCounts = WordCountRddAppV2.countWords(lines).collect()
    assert(wordCounts.contains(("four", 2)))
    assert(wordCounts.size == 4)
  }
}
