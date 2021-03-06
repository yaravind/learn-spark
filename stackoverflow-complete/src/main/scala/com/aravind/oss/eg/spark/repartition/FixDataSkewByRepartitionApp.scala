package com.aravind.oss.eg.spark.repartition

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Demonstrates data skew.
 * Takes around 4 minutes to complete as this is run by a single thread.
 */
object FixDataSkewByRepartitionApp extends App with Logging {
  val spark = getSparkSession("FixDataSkewByRepartitionApp", getClusterCfg(args))

  val userDF = spark.read.option("header", "true").csv("datasets/users.csv.gz")
  userDF.createOrReplaceTempView("users")
  logInfo("Schema - USERS")
  userDF.printSchema()
  logInfo("userDF num partitions: " + userDF.rdd.getNumPartitions)
  userDF.show(3)
  logInfo("User count: " + userDF.count())

  val deptDF = spark.read.option("header", "true").csv("datasets/depts.csv.gz")
  deptDF.createOrReplaceTempView("depts")
  logInfo("Schema - DEPARTMENTS")
  deptDF.printSchema()
  logInfo("deptDF num partitions: " + deptDF.rdd.getNumPartitions)
  deptDF.show(3)
  logInfo("Department count: " + deptDF.count())

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