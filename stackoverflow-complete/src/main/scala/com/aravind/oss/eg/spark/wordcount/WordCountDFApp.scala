package com.aravind.oss.eg.spark.wordcount

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import com.aravind.oss.eg.spark.wordcount.WordCountUtil._
import org.apache.spark.sql.DataFrame

/** *
 * Need to explicitly import <code>import linesDf.sparkSession.implicits._</code> in functions
 * like <code>toWords</code> to avoid runtime exceptions during unit testing. Refer
 * https://stackoverflow.com/questions/61166287/nullpointerexception-when-referencing-dataframe-column-names-with-method-call
 */
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

  import org.apache.spark.sql.functions._

  val wordsDf = toWords(linesDf)

  logInfo("Inferred schema")
  wordsDf.printSchema()

  logInfo("DataFrame after splitting the line into words")
  wordsDf.show(false)

  countWords(wordsDf).show(false)

  def toWords(linesDf: DataFrame) = {
    import linesDf.sparkSession.implicits._
    linesDf
      .select($"line",
        explode(split($"line", WhitespaceRegex)).as("word"))
  }

  def countWords(wordsDf: DataFrame): DataFrame = {
    import wordsDf.sparkSession.implicits._
    wordsDf.filter($"word" =!= "")
      .groupBy(lower($"word"))
      .count()
  }
}