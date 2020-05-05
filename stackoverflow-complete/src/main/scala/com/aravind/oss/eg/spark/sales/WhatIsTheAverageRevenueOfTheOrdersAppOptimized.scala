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
  val spark = getSparkSession("HowManyProductsHaveBeenSoldAtLeastOnceApp", getClusterCfg(args))
  val orderDF = loadSales()
  val productDF = loadProducts()

  //  val result = run(spark)
  //  result.show(15)

  val skewedSqlText =
    """ SELECT product_id, count(product_id) AS cnt
      | FROM orders
      | GROUP BY product_id
      | ORDER BY cnt DESC
      |""".stripMargin

  logInfo("Getting skewed id's from skewed table: ORDERS")
  val skewedProducts: Array[Row] = spark.sql(skewedSqlText)
    .limit(1) //only want 1 skewed product
    .collect()

  val skewedKeys: Array[String] = skewedProducts.map(r => r.getAs[String]("product_id"))

  logInfo("Skewed keys")
  skewedKeys.foreach(k => println("key: " + k))

  logInfo("Original 'Unskewed' DataFrame with salted keys ")
  val saltedProductsDF = replicateNonSkewedData(productDF, "product_id", skewedKeys, 50)(spark)
  saltedProductsDF.createOrReplaceTempView("PRODUCTS_SALTED")
  saltedProductsDF.show(5)

  val saltedOrderDF = saltSkewedData(orderDF, "product_id", skewedKeys, 50)(spark)
  saltedOrderDF.createOrReplaceTempView("ORDERS_SALTED")
  saltedOrderDF.show(5)

  val result = run(spark)
  result.show()

  def run(spark: SparkSession): DataFrame = {
    /**
     * total_revenue = sum(num_pieces_sold * unit_price)
     * avg_revenue = total_revenue / orders_count
     */
    val sqlText =
      """ SELECT avg(p.price * o.num_pieces_sold) as total_revenue
        | FROM orders_salted o
        | JOIN products_salted p ON p.salted_product_id = o.salted_key1
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

  def saltSkewedData(inputDF: DataFrame, inputKeyColName: String, skewedKeys: Array[String], replicationFactor: Int)(spark: SparkSession): DataFrame = {
    val keyCol = inputDF(inputKeyColName)
    val saltedInputDF = inputDF.withColumn("salted_key1",
      when(
        keyCol.isInCollection(skewedKeys),
        concat(keyCol, lit("-"), round(rand() * (replicationFactor - 1), 0).cast(IntegerType))
      ).otherwise(
        keyCol
      ))

    saltedInputDF
  }
}
