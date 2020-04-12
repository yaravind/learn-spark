package com.aravind.oss

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  /** It's typically best set the number of shuffle partitions to one in your test suite.
   * This configuration can make your tests run up to 70% faster. You can remove this configuration option
   * or adjust it if you're working with big DataFrames in your test suite. **/
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark-fast-tests test session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}
