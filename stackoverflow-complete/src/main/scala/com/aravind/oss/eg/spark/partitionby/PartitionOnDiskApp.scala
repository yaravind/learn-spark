package com.aravind.oss.eg.spark.partitionby

import com.aravind.oss.eg.spark.SparkAppUtil.{activeExecutors, getSparkSession}
import com.aravind.oss.{DataFrameUtils, Logging}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.spark.sql.functions.{col, collect_list, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Date

object PartitionOnDiskApp extends App with Logging with DataFrameUtils {
  logInfo("PartitionOnDiskApp")
  val spark = getSparkSession("PartitionOnDiskApp", "local[4]")

  logInfo(s"Cluster: ${args(0)}")
  logInfo(s"Active executors: ${activeExecutors(spark)}")

  var df = spark.read.option("header", "true").csv("datasets/partition_by/test.csv")

  df.show()
  df = addLoadDateTime(df)
  df.show()

  /*
  - business_key: natural key
  - hash_key: is based on the business key, MD5 (recommended)
  - load_date_time: The load date indicates when the business key initially arrived in the data warehouse.
      It is a system-generated and system-maintained field.
  - last_seen_date: The last seen date indicates when the business key was “last seen” in the source system. Every
      business key in the current batch is updated with the batch date as the last seen date. If a business key is
      not in the current batch, its last seen date is not modified. All business keys that have a last seen date
      that is below a configured threshold are considered to be deleted.
  - record_source: file_path()

   */
  def addLoadDateTime(df: DataFrame): DataFrame = {
    val datePart = DateFormatUtils.format(new Date(), "YYYY-MM-dd-HH-mm-ss")
    println("--->YYYY-MM-dd-HH-mmZ=" + DateFormatUtils.format(new Date(), "YYYY-MM-dd-HH-mmZ"))
    println("--->YYYY-MM-dd-HH-mmZZ\"=" + DateFormatUtils.format(new Date(), "YYYY-MM-dd-HH-mmZZ"))
    println("--->yyyy-MM-dd'T'HH:mmZZ=" + DateFormatUtils.format(new Date(), "yyyy-MM-dd'T'HH:mmZZ"))

    println("---->" + Instant.now().toString())
    df.withColumn("ldt", lit(datePart))
  }

  logInfo(s"In-memory partition count: ${df.rdd.getNumPartitions}")
  logInfo("Charset from StandardCharsets.UTF_8.name: " + StandardCharsets.UTF_8.name())

  getRecordsPerPartition(df).show()

  //writeAsCsv(spark)(df, "out/partition_by/PartitionOnDiskApp/test", "ldt", numPartitions = 1)

  saveCsvAsTable(spark)(df, "testdb", "test_table", "ldt", "This is a test table.", numPartitions = 1)

  spark.sql("DESCRIBE TABLE EXTENDED testdb.test_table").show(truncate = false)

  if (isTablePartitioned(spark, "testdb.test_table")) {
    val partitionKeys = getPartitionColumns(spark, "testdb.test_table")
    logInfo(s"Table is partitioned. Total partition keys: ${partitionKeys.length}")
    partitionKeys.foreach(key => logInfo(s"Key: $key"))
  } else {
    logInfo("Table is not partitioned.")
  }

  def saveCsvAsTable(spark: SparkSession)(df: DataFrame, dbName: String, tableName: String, partitionCol: String, comment: String, numPartitions: Int = -1,
                                          saveMode: SaveMode = SaveMode.Append, otherWriterOpts: Map[String, String] = Map.empty): Unit = {
    val fullTableName = s"$dbName.$tableName"
    logInfo(s"Saving as : $fullTableName, at location: ${spark.conf.get("spark.sql.warehouse.dir")}, with partition col: $partitionCol")

    if (!spark.catalog.databaseExists(dbName)) {
      val userName = getOrElse(mssparkutils.env.getUserName())
      val userId = getOrElse(mssparkutils.env.getUserId())
      val jobId = getOrElse(mssparkutils.env.getJobId())
      val pool = getOrElse(mssparkutils.env.getPoolName())
      val workspace = getOrElse(mssparkutils.env.getWorkspaceName())
      val cluster = getOrElse(mssparkutils.env.getClusterId())

      val props =
        s"""createdByName = '$userName',
           | createdById = '$userId',
           | jobId = '$jobId',
           | sparkPool = '$pool',
           | workspaceName  = '$workspace',
           | clusterId = '$cluster'"""

      logInfo(
        s"""$dbName doesn't exist.
           |Creating with props (
           | $props
           |).""".stripMargin)

      spark.sql(
        s"""CREATE DATABASE IF NOT EXISTS $dbName
           |COMMENT '$comment'
           |WITH DBPROPERTIES (
           | $props
           |)
           |""".stripMargin)
    }
    val tempDf = selectCoalesceOrRepartition(df, numPartitions)

    if (!spark.catalog.tableExists(fullTableName)) {
      logInfo(s"$fullTableName doesn't exist in catalog. Creating it.")
      tempDf
        .write
        .partitionBy(partitionCol, "country")
        .options(otherWriterOpts)
        .option("header", true)
        .option("encoding", StandardCharsets.UTF_8.name())
        .mode(saveMode)
        //.insertInto(fullTableName)
        .saveAsTable(fullTableName)
    }
    else {
      logInfo(s"$fullTableName already exists in catalog. Appending to it.")
      tempDf
        .write
        //.partitionBy(partitionCol)
        .options(otherWriterOpts)
        .option("header", true)
        .option("encoding", StandardCharsets.UTF_8.name())
        .mode(saveMode)
        .insertInto(fullTableName)
      //.saveAsTable(fullTableName)
    }

  }

  def getOrElse(in: String, default: String = "Not Available"): String = {
    if (in.isEmpty) default else in
  }

  def writeAsCsv(spark: SparkSession)(df: DataFrame, writeAbsPath: String, partitionCol: String, numPartitions: Int = -1,
                                      saveMode: SaveMode = SaveMode.Append, otherWriterOpts: Map[String, String] = Map.empty): Unit = {
    logInfo(s"Writing to location: $writeAbsPath with partition col: $partitionCol")

    val tempDf = selectCoalesceOrRepartition(df, numPartitions)

    tempDf
      .write
      .partitionBy(partitionCol)
      .options(otherWriterOpts)
      .option("header", true)
      .option("encoding", StandardCharsets.UTF_8.name())
      .mode(saveMode)
      .csv(writeAbsPath)
  }


  def selectCoalesceOrRepartition(df: DataFrame, numPartitions: Int): DataFrame = {
    var tempDf = df
    numPartitions match {
      case n if (n == 1) => {
        tempDf = df.coalesce(1)
        logInfo(s"Coalescing to single partition before save")
      }
      case n if (n > 1) => {
        tempDf = df.repartition(numPartitions)
        logInfo(s"Repartitioning to $numPartitions before save")
      }
      case _ => logInfo(s"Keeping current partitions before save: ${df.rdd.getNumPartitions}")
    }

    tempDf
  }

  /** *
   * This function depends on the output from DESCRIBE EXTENDED table_name sql command which produces following
   *
   * +----------------------------+-------------------------------------------------------------------------------------------------------+-------+
   * |col_name                    |data_type                                                                                              |comment|
   * +----------------------------+-------------------------------------------------------------------------------------------------------+-------+
   * |first_name                  |string                                                                                                 |null   |
   * |last_name                   |string                                                                                                 |null   |
   * |country                     |string                                                                                                 |null   |
   * |ldt                         |string                                                                                                 |null   |
   * |# Partition Information     |                                                                                                       |       |
   * |# col_name                  |data_type                                                                                              |comment|
   * |ldt                         |string                                                                                                 |null   |
   * |                            |                                                                                                       |       |
   * |# Detailed Table Information|                                                                                                       |       |
   * |Database                    |testdb                                                                                                 |       |
   * |Table                       |test_table                                                                                             |       |
   * |Created Time                |Sun Nov 13 19:38:45 EST 2022                                                                           |       |
   * |Last Access                 |Wed Dec 31 18:59:59 EST 1969                                                                           |       |
   * |Created By                  |Spark 2.4.3                                                                                            |       |
   * |Type                        |MANAGED                                                                                                |       |
   * |Provider                    |parquet                                                                                                |       |
   * |Location                    |file:/Users/o60774/Documents/WS/learn-spark/stackoverflow-complete/spark-warehouse/testdb.db/test_table|       |
   * |Storage Properties          |[encoding=UTF-8, header=true]                                                                          |       |
   * |Partition Provider          |Catalog                                                                                                |       |
   * +----------------------------+-------------------------------------------------------------------------------------------------------+-------+
   *
   * @param spark
   *
   * @param table
   * @return
   */
  def isTablePartitioned(spark: SparkSession, table: String): Boolean = {
    /*
    Need to explicitly import encoders to avoid the following error

    Unable to find encoder for type Array[String]. An implicit Encoder[Array[String]] is
    needed to store Array[String] instances in a Dataset.
     */
    import spark.implicits._
    val colDetails = spark
      .sql(s" describe extended ${table} ")
      .select("col_name")
      .select(collect_list(col("col_name")))
      .as[Array[String]].first

    colDetails.filter(x => x.contains("# Partition Information")).length > 0
  }

  def getPartitionColumns(spark: SparkSession, table: String): Array[String] = {
    import spark.implicits._
    val pat = """(?ms)^\s*#( Partition Information)(.+)(Detailed Table Information)\s*$""".r
    val colDetails = spark.sql(s" describe extended ${table} ")
      .select("col_name")
      .select(collect_list(col("col_name")))
      .as[Array[String]].first

    val colDetails2 = colDetails.filter(_.trim.length > 0).mkString("\n")

    val arr = pat.findAllIn(colDetails2)
      .matchData.collect { case pat(a, b, c) => b }
      .toList(0).split("\n")
      .filterNot(x => x.contains("#")).filter(_.length > 0)

    arr
  }
}

