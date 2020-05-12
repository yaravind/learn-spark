package com.aravind.oss.eg.spark

import org.apache.spark.sql.SparkSession

object SparkAppUtil {
  def getSparkSession(appName: String, clusterCfg: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(clusterCfg).getOrCreate()
  }

  def getClusterCfg(args: Array[String]): String = {
    if (!args.isEmpty && args.length > 1) args(1) else "local[*]"
  }

  def activeExecutors(spark: SparkSession): Seq[String] = {
    val allExecutors = spark.sparkContext.getExecutorMemoryStatus.map(_._1)
    val driverHost: String = spark.sparkContext.getConf.get("spark.driver.host")
    allExecutors.filter(!_.split(":")(0).equals(driverHost)).toList
  }
}
