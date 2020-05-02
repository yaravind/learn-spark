package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.loadSales
import org.apache.spark.sql.SparkSession

/** *
 * Problem 2: Which is the product contained in more orders?
 */
object WhichIsTheProductContainedInMoreOrdersApp extends App with Logging {
  val spark = getSparkSession("HowManyProductsHaveBeenSoldAtLeastOnceApp", getClusterCfg(args))
  val orderDF = loadSales()
  orderDF.show(5)

  val product_id = run(spark)
  logInfo("A (should be product_id '0', which is sold 19000000 times): " + product_id)

  def run(spark: SparkSession): String = {
    if (!spark.catalog.tableExists("ORDERS")) {
      logError("Table ORDERS doesn't exist")
      return "";
    }

    val sqlText =
      """ SELECT product_id, count(product_id) AS sold_cnt
        | FROM orders
        | GROUP BY product_id
        | ORDER BY sold_cnt DESC
        |""".stripMargin

    val resDF = spark.sql(sqlText)
    logInfo("Top 5 products sold: ")
    resDF.show(5)
    resDF.select(resDF("product_id")).take(1)(0).getAs[String](0)
  }
}
