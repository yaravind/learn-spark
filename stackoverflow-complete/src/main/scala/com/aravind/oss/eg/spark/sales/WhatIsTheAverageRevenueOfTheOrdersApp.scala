package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.{loadProducts, loadSales}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Problem 4: What is the average revenue of the orders?
 */
object WhatIsTheAverageRevenueOfTheOrdersApp extends App with Logging {
  val spark = getSparkSession("HowManyProductsHaveBeenSoldAtLeastOnceApp", getClusterCfg(args))
  val orderDF = loadSales()
  val productDF = loadProducts()

  val result = run(spark)
  result.show(15)

  def run(spark: SparkSession): DataFrame = {
    /**
     * total_revenue = sum(num_pieces_sold * unit_price)
     * avg_revenue = total_revenue / orders_count
     */
    val sqlText =
      """ SELECT avg(p.price * o.num_pieces_sold) as total_revenue
        | FROM orders o
        | JOIN PRODUCTS p ON p.product_id = o.product_id
        |""".stripMargin
    val result = spark.sql(sqlText)

    result
  }

}
