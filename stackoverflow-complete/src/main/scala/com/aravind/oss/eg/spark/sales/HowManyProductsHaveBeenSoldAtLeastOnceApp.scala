package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.{loadProducts, loadSales}
import org.apache.spark.sql.SparkSession

/**
 * TODO: replace tableExists with requireTableExists
 * Problem 1: How many products have been sold at least once?
 */
object HowManyProductsHaveBeenSoldAtLeastOnceApp extends App with Logging {

  val spark = getSparkSession("HowManyProductsHaveBeenSoldAtLeastOnceApp", getClusterCfg(args))

  val productDF = loadProducts()
  productDF.show(5)

  val orderDF = loadSales()
  orderDF.show(5)

  val count = run(spark)
  logInfo("A (should be 993429): " + count)

  def run(spark: SparkSession): Long = {
    if (!spark.catalog.tableExists("PRODUCTS")) {
      logError("Table PRODUCTS doesn't exist")
      return -1
    }
    if (!spark.catalog.tableExists("ORDERS")) {
      logError("Table ORDERS doesn't exist")
      return -1
    }
    val withJoin =
      """ SELECT distinct o.product_id
        | FROM PRODUCTS p
        | JOIN ORDERS o ON p.product_id = o.product_id
        |""".stripMargin

    val count = spark.sql(withJoin).count()

    count
  }
}
