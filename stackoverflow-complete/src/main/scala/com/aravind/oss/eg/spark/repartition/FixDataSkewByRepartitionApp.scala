package com.aravind.oss.eg.spark.repartition

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Demonstrates using repartitioning technique to fix data skew.
 */
object FixDataSkewByRepartitionApp extends App with Logging {
  val spark = getSparkSession("FixDataSkewByRepartitionApp", getClusterCfg(args))

  val userDF = spark.read.option("header", "true").csv("datasets/users.csv.gz")
  userDF.createOrReplaceTempView("users")
  logInfo("Schema - USERS")
  userDF.printSchema()
  userDF.show(3)
  logInfo("User count: " + userDF.count())

  val deptDF = spark.read.option("header", "true").csv("datasets/depts.csv.gz")
  deptDF.createOrReplaceTempView("depts")
  logInfo("Schema - DEPARTMENTS")
  deptDF.printSchema()
  deptDF.show(3)
  logInfo("Department count: " + deptDF.count())

  logInfo("userDF num partitions: " + userDF.rdd.getNumPartitions)
  logInfo("deptDF num partitions: " + deptDF.rdd.getNumPartitions)

  spark.sql(
    """ SELECT department_id, count(*) AS count
      | FROM users
      | GROUP BY department_id
      | ORDER BY count DESC
      |""".stripMargin).show(15)


  //  val userCountByDept = run(spark)
  //
  //  logInfo("User count by Department: ")
  //  userCountByDept.show(5)

  def run(spark: SparkSession): DataFrame = {
    if (!spark.catalog.tableExists("users")) {
      logError("Table 'users' doesn't exist")
    }
    if (!spark.catalog.tableExists("depts")) {
      logError("Table 'depts' doesn't exist")
    }
    val withJoin =
      """ SELECT first(d.id), first(d.name), count(u.id) AS user_count
        | FROM depts d
        | JOIN users u ON d.id = u.department_id
        |""".stripMargin

    val result = spark.sql(withJoin)

    result
  }
}