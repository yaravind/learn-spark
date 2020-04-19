package com.aravind.oss.eg.spark.wordcount

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import com.aravind.oss.eg.spark.wordcount.WordCountUtil.{WhitespaceRegex, getPaths}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{explode, split}

object WordCountDSApp extends App with Logging {
  logInfo("WordCount with Dataset API and multiple Case classes")
  val paths = getPaths(args)
  val cluster = getClusterCfg(args)

  if (paths.size > 1) {
    logInfo("More than one file to process")
  }
  logInfo("Path(s): " + paths)
  logInfo("Cluster: " + cluster)

  val spark = getSparkSession("WordCountDSApp", cluster)

  import spark.implicits._

  /*
  case class <code>Line<code> SHOULD match the number of columns in the input file
   */
  val linesDs: Dataset[Line] = spark.read
    .textFile(paths: _*)
    .toDF("line")
    .as[Line]
  logInfo("Dataset before splitting line")
  linesDs.show(false)

  /*
  <code>toWords<code> adds additional column (word) to the output so we need a
  new case class <code>LineAndWord</code> that contains two properties to represent two columns.
  The names of the properties should match the name of the columns as well.
   */
  val wordDs: Dataset[LineAndWord] = toWords(linesDs)
  logInfo("Dataset after splitting the line into words")
  wordDs.show(false)

  val wordCount = countWords(wordDs)
  wordCount
    .orderBy($"count(1)".desc)
    .show(false)

  def toWords(linesDs: Dataset[Line]): Dataset[LineAndWord] = {
    import linesDs.sparkSession.implicits._
    linesDs
      .select($"line",
        explode(split($"line", WhitespaceRegex)).as("word"))
      .as[LineAndWord]
  }

  def countWords(wordsDs: Dataset[LineAndWord]): Dataset[(String, Long)] = {
    import wordsDs.sparkSession.implicits._
    val result = wordsDs
      .filter(_.word != null)
      .filter(!_.word.isEmpty)
      .groupByKey(_.word.toLowerCase)
      .count()

    result
  }

  case class Line(line: String)

  case class LineAndWord(line: String, word: String)

}
