package com.aravind.oss.eg.spark.sales

import java.math.BigInteger
import java.security.MessageDigest

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import com.aravind.oss.eg.spark.sales.ProductSalesUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Problem: Share example of UDF
 *
 * <ul>
 * <li>
 * if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text.
 *    E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
 * </li>
 * <li>
 * if the order_id is odd: apply SHA256 hashing to the bill text
 * Finally, check if there are any duplicate on the new column
 * </li>
 * </ul>
 */
object HashBillAndFindDuplicatesApp extends App with Logging {
  val MD5Algo = MessageDigest.getInstance("MD5")
  val SHA256Algo = MessageDigest.getInstance("SHA-256")
  val spark = getSparkSession("HashBillAndFindDuplicatesApp", getClusterCfg(args))

  //register UDF's in the session
  spark.udf.register("md5", md5HashString)
  spark.udf.register("isEven", isEven)
  spark.udf.register("sha256", sha256HashString)
  spark.udf.register("hash", hash)

  val orderDF = loadSales()
  orderDF.select("order_id", "bill_raw_text").show(5, truncate = true)

  val result = run(spark)
  result.show(5)

  val resultTwo = runTwo(spark)
  resultTwo.show(25)

  logInfo("Execution plan:")
  println(result.queryExecution.executedPlan)

  logInfo("Duplicates")
  //Detect duplicates
  resultTwo
    .groupBy("hash")
    .agg(count("*").alias("count"))
    .where(col("count").gt(1))
    .show()

  def runTwo(spark: SparkSession): DataFrame = {
    val sqlTxt =
      """
        | SELECT
        |   order_id,
        |   bill_raw_text,
        |   IF(isEven(order_id), 'MD5', 'sha256') AS hash_type,
        |   hash(order_id, bill_raw_text) AS hash
        | FROM
        |   orders
        |""".stripMargin

    val result = spark.sql(sqlTxt)

    result
  }

  def run(spark: SparkSession): DataFrame = {
    val sqlTxt =
      """
        | SELECT
        |   order_id,
        |   bill_raw_text,
        |    CASE
        |     WHEN isEven(order_id)
        |     THEN 'md5'
        |     ELSE 'sha256'
        |   END AS hash_type,
        |   CASE
        |     WHEN isEven(order_id)
        |     THEN md5(bill_raw_text)
        |     ELSE sha256(bill_raw_text)
        |   END AS hash
        | FROM
        |   orders
        |""".stripMargin

    val result = spark.sql(sqlTxt)

    result
  }

  def isEven: String => Boolean = (s: String) => {
    if (s.toInt % 2 == 0) true else false
  }

  def md5HashString: String => String = (s: String) => {
    val digest = MD5Algo.digest(s.getBytes("UTF-8"))
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }

  def sha256HashString: String => String = (s: String) => {
    val digest = SHA256Algo.digest(s.getBytes("UTF-8"))
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }

  def hash: (String, String) => String = (orderId: String, bill: String) => {
    //if the order_id is even: apply MD5 hashing iteratively to
    // the bill_raw_text field, once for each 'A' (capital 'A') present in the text.
    if (orderId.toInt % 2 == 0) {
      val times = bill.count(_ == 'A')
      println("A occurs times: " + times)
      var hash = md5HashString(bill)
      for (i <- 1 until times) hash = md5HashString(bill)
      hash
    } else {
      sha256HashString(bill)
    }
  }
}
