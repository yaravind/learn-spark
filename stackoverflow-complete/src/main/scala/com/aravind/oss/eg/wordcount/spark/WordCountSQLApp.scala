package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.Logging
import com.aravind.oss.eg.wordcount.spark.WordCountUtil._
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCountSQLApp extends App with Logging {
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
  linesDf.createOrReplaceTempView("lines_tbl")

  logInfo("DataFrame before splitting line")
  spark.sql("SELECT * FROM lines_tbl").show(false)

  splitToWords(spark)

  logInfo("Inferred schema")
  spark.sql("desc words_tbl").show()

  logInfo("DataFrame after splitting the line into words")
  spark.sql("SELECT * FROM words_tbl").show(false)

  countWords(spark).show(false)

  def splitToWords(spark: SparkSession): Unit = {
    spark
      .sql("""SELECT line, explode(split(line,"[\\s]")) as word FROM lines_tbl""")
      .createOrReplaceTempView("words_tbl")
  }

  def countWords(spark: SparkSession): DataFrame = {
    spark.sql(
      """
        | SELECT lower(word), count(lower(word)) AS count
        | FROM words_tbl
        | WHERE word is not null AND word != ""
        | GROUP BY lower(word)
        | ORDER BY count DESC
        | """.stripMargin)
  }
}