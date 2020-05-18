package com.aravind.oss.eg.spark.repartition

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{activeExecutorCount, getClusterCfg, getSparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Demonstrates using repartitioning technique to fix data skew.
 * Completes with in 30 secs.
 * Reference: https://github.com/dataengi/spark-challenges/blob/master/src/main/scala/DataSkew.scala
 */
object FixDataSkewByRepartitionAppOptimized1 extends App with Logging {
  val spark = getSparkSession("FixDataSkewByRepartitionApp", getClusterCfg(args))

  val userDF = spark.read.option("header", "true").csv("datasets/users.csv.gz")
  logInfo("userDF num partitions (BEFORE): " + userDF.rdd.getNumPartitions)

  // Change 1: ********* Repartition
  val MultiplicationFactor = 2
  val userDFOptimized = userDF.repartition(activeExecutorCount(spark) * MultiplicationFactor)
  userDFOptimized.createOrReplaceTempView("users")
  logInfo("userDF num partitions (AFTER): " + userDFOptimized.rdd.getNumPartitions)

  val deptDF = spark.read.option("header", "true").csv("datasets/depts.csv.gz")
  // Change 2: ********* sort
  val deptDFOptimized = deptDF.sort("id", "assigned_date", "company_id", "factory_id")
  deptDF.createOrReplaceTempView("depts")

  val userCountByDept = run(spark)

  logInfo("Count of all users whose joined a department on the same day their birth date?: ")
  userCountByDept.show(5)

  def run(spark: SparkSession): DataFrame = {
    if (!spark.catalog.tableExists("users")) {
      logError("Table 'users' doesn't exist")
    }
    if (!spark.catalog.tableExists("depts")) {
      logError("Table 'depts' doesn't exist")
    }

    //All users whose joined a department on the same day their birth date?
    val withJoin =
      """
        | SELECT *
        | FROM depts d
        | JOIN users u ON u.department_id = d.id
		|		AND u.date_of_birth = d.assigned_date
		|		AND (
		|				u.company_id = d.company_id
		|				OR u.company_id = d.factory_id
		|		)
        |""".stripMargin

    val result = spark.sql(withJoin)

    result
  }
}