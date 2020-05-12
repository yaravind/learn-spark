package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.{loadProducts, loadSales}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Problem 4: What is the average revenue of the orders?
 *
 * <p>
 * Fixes the data skew issue with <b>product_id 0</b> by using partial salting technique.
 * </p>
 *
 *
 */
object WhatIsTheAverageRevenueOfTheOrdersAppOptimized extends App with Logging {
  val spark = getSparkSession("WhatIsTheAverageRevenueOfTheOrdersAppOptimized", getClusterCfg(args))
  val orderDF = loadSales()
  val productDF = loadProducts()

  logInfo("Q: What is the average revenue of the orders?")
  val result = run(spark)
  logInfo("A: ")
  result.show()

  logInfo("Q: What is the count of each of the products sold?")
  val countResult = runCount(spark)
  logInfo("A: ")
  countResult.show()

  println("Complete")

  /**
   * We need to handle the group level aggregation differently, by sub-selects
   *
   * @param spark
   * @return
   */
  def runCount(spark: SparkSession): DataFrame = {
    val sqlTxt =
      """ SELECT key1, count(*)
        | FROM (
        |       SELECT split(fact.salted_key,"-")[0] AS key1
        |       FROM orders_salted fact, products_salted dim
        |       WHERE fact.salted_key = dim.salted_product_id
        | )
        | GROUP BY key1
        | ORDER BY key1
        |""".stripMargin

    val result = spark.sql(sqlTxt)

    result
  }

  def run(spark: SparkSession): DataFrame = {
    val skewedSqlText =
      """ SELECT product_id, count(product_id) AS cnt
        | FROM orders
        | GROUP BY product_id
        | ORDER BY cnt DESC
        |""".stripMargin

    logInfo("Getting skewed id's from skewed table: ORDERS")
    val skewedProducts: Array[Row] = spark.sql(skewedSqlText)
      .limit(1) //limit only to 1 skewed product as there is only one skewed product in this dataset
      .collect()

    val skewedKeys: Array[String] = skewedProducts.map(r => r.getAs[String]("product_id"))
    logInfo("Skewed keys")
    skewedKeys.foreach(k => logInfo("key: " + k))

    val ReplicationFactor = 150
    val skewedKeyColName = "product_id"

    val saltedProductsDF = replicateNonSkewedData(productDF, skewedKeyColName, skewedKeys, ReplicationFactor)(spark)
    saltedProductsDF.createOrReplaceTempView("PRODUCTS_SALTED")
    logInfo("'Un-skewed' DataFrame with salted keys ")
    saltedProductsDF.show(5)

    val saltedOrderDF = saltSkewedData(orderDF, skewedKeyColName, skewedKeys, ReplicationFactor)(spark)
    saltedOrderDF.createOrReplaceTempView("ORDERS_SALTED")
    logInfo("'Skewed' DataFrame with salted keys ")
    saltedOrderDF.show(5)
    /**
     * total_revenue = sum(num_pieces_sold * unit_price)
     * avg_revenue = total_revenue / orders_count
     *
     * Use the SALTED DataFrames registered
     */
    val sqlText =
      """ SELECT avg(p.price * o.num_pieces_sold) as avg_revenue
        | FROM orders_salted o
        | JOIN products_salted p ON p.salted_product_id = o.salted_key
        |""".stripMargin
    val result = spark.sql(sqlText)

    result
  }

  def replicateNonSkewedData(inputDF: DataFrame, inputKeyColName: String, skewedKeys: Array[String], replicationFactor: Int)(spark: SparkSession): DataFrame = {
    val repl: Array[(String, String)] = for {
      originalKey <- skewedKeys
      repl <- 0 until replicationFactor
    } yield ((originalKey, repl.toString))

    //repl.foreach(println)

    val replDF = spark
      .createDataFrame(spark.sparkContext.parallelize(repl))
      .toDF("original_key", "salt")

    val keyCol = inputDF(inputKeyColName)

    val saltedInputDF = inputDF
      .join(
        broadcast(replDF),
        keyCol === replDF("original_key"),
        "left")
      .withColumn("salted_" + inputKeyColName,
        when(
          replDF("salt").isNull, keyCol)
          .otherwise(concat(keyCol, lit("-"), replDF("salt"))))
      .drop("original_key", "salt") //remove unnecessary columns

    saltedInputDF
  }

  def saltSkewedData[T](inputDF: DataFrame, inputKeyColName: String, skewedKeys: Array[T], replicationFactor: Int)(spark: SparkSession): DataFrame = {
    val keyCol = inputDF(inputKeyColName)
    val saltedInputDF = inputDF.withColumn("salted_key",
      when(
        keyCol.isInCollection(skewedKeys),
        concat(keyCol, lit("-"), round(rand() * (replicationFactor - 1), 0).cast(IntegerType))
      ).otherwise(
        keyCol
      ))

    saltedInputDF
  }
}
