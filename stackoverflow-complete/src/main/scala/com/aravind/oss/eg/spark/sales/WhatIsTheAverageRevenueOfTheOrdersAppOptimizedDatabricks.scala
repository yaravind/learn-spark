package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.{loadProducts, loadSales}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Problem 4: What is the average revenue of the orders?
 *
 * <p>
 * Fixes the data skew issue with <b>product_id 0</b> by using out of the
 * box <b>skew</b> hint provided by <b>Databricks only</b>.
 *
 * SQL Reference: https://docs.databricks.com/delta/join-performance/skew-join.html#relation-columns-and-skew-values
 * DataFrame Reference: https://kb.databricks.com/data/skew-hints-in-join.html
 * </p>
 */
object WhatIsTheAverageRevenueOfTheOrdersAppOptimizedDatabricks extends App with Logging {
  val spark = getSparkSession("WhatIsTheAverageRevenueOfTheOrdersAppOptimizedDatabricks", getClusterCfg(args))
  val orderDF = loadSales()
  val productDF = loadProducts()

  val skewedKeyColName = "product_id"
  val optimizedOrderDF = orderDF.hint("skew", skewedKeyColName)
  optimizedOrderDF.createOrReplaceTempView("ORDERS_OPTIMIZED")

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
      """ SELECT fact.product_id, count(*) AS count
        | FROM orders_optimized fact, products dim
        | WHERE fact.product_id = dim.product_id
        | GROUP BY fact.product_id
        | ORDER BY fact.product_id desc
        |""".stripMargin

    val result = spark.sql(sqlTxt)

    result
  }

  def run(spark: SparkSession): DataFrame = {
    /**
     * total_revenue = sum(num_pieces_sold * unit_price)
     * avg_revenue = total_revenue / orders_count
     *
     * Use the SALTED DataFrames registered
     */
    val sqlText =
      """ SELECT avg(p.price * o.num_pieces_sold) as avg_revenue
        | FROM orders_optimized o
        | JOIN products p ON p.product_id = o.product_id
        |""".stripMargin
    val result = spark.sql(sqlText)

    result
  }
}
