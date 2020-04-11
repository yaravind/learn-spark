package com.aravind.oss.eg.wordcount.spark

import com.aravind.oss.eg.wordcount.spark.WordCountRddAppV2.logInfo
import org.apache.spark.sql.SparkSession

object WordCountUtil {

  def getSparkSession(appName: String, clusterCfg: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(clusterCfg).getOrCreate()
  }

  def getPaths(args: Array[String]): Seq[String] = {
    val paths: Seq[String] = if (args(0) != null) {
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
    if (args(1) != null) args(1) else "local[*]"
  }
}
