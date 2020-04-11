package com.aravind.oss.eg.wordcount.spark

import org.apache.spark.sql.SparkSession

object WordCountUtil {
  //matches a space, a tab, a carriage return, a line feed, or a form feed
  val WhitespaceRegex = "[\\s]"

  def getSparkSession(appName: String, clusterCfg: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(clusterCfg).getOrCreate()
  }

  def getPaths(args: Array[String]): Seq[String] = {
    val paths: Seq[String] = if (!args.isEmpty) {
      args(0)
        .split(",")
        .map(x => x.trim)
        .filter(x => !x.isEmpty)
    }
    else {
      Seq("src/main/resources/wordcount/test.txt")
    }

    paths
  }

  def getClusterCfg(args: Array[String]): String = {
    if (!args.isEmpty && args(1) != null) args(1) else "local[*]"
  }
}
