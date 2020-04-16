package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.Logging
import com.aravind.oss.eg.wordcount.spark.WordCountUtil.{WhitespaceRegex, getClusterCfg, getPaths, getSparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{explode, split}

/** *
 * The file test_ds_v2.txt differes from test.txt as follows
 *
 * 1. A comma has been added at end of each line to make sure spark reader thinks there are two columns in the file
 * as that is what is expected (because case class <code>Line<code> has two properties.
 *
 * <p>
 * When dealing with dataset API, there is a difference between "missing columns" and "missing values"
 * </p>
 *
 * Reference: https://medium.com/balabit-unsupervised/spark-scala-dataset-tutorial-c97415c5034
 */
object WordCountDSAppV2 extends App with Logging {
  logInfo("WordCount with Dataset API and single Case class")
  val paths = getPaths(args)
  val cluster = getClusterCfg(args)

  if (paths.size > 1) {
    logInfo("More than one file to process")
  }
  logInfo("Path(s): " + paths)
  logInfo("Cluster: " + cluster)

  val spark = getSparkSession("WordCountDSAppV2", cluster)

  import spark.implicits._

  val lineDf = spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv(paths: _*)
    .toDF("line", "word")

  lineDf.printSchema

  val linesDs: Dataset[Line] = lineDf.as[Line]
  logInfo("Dataset before splitting line")
  linesDs.show(false)

  val wordDs: Dataset[Line] = toWords(linesDs)
  logInfo("Dataset after splitting the line into words")
  wordDs.show(false)

  val wordCount = countWords(wordDs)
  wordCount
    .orderBy($"count(1)".desc)
    .show(false)

  def toWords(linesDs: Dataset[Line]): Dataset[Line] = {
    import linesDs.sparkSession.implicits._
    linesDs
      .select($"line",
        explode(split($"line", WhitespaceRegex)).as("word"))
      .as[Line]
  }

  def countWords(wordsDs: Dataset[Line]): Dataset[(String, Long)] = {
    import wordsDs.sparkSession.implicits._
    val result = wordsDs
      //.filter(_.word != null) will return wrong results as we aren't using getOrElse
      //.filter(_.word..isEmpty) will return wrong results as we aren't using getOrElse
      .filter(!_.word.getOrElse("").isEmpty)
      .groupByKey(_.word.getOrElse("").toLowerCase)
      .count()

    result
  }

  /**
   * Missing values need to be handled by using <code>Option<code> class.
   * Reference: https://medium.com/balabit-unsupervised/spark-scala-dataset-tutorial-c97415c5034
   *
   * @param line
   * @param word need to be made optional as words are not going to be present in the original file.
   */
  case class Line(line: String, word: Option[String])

}
