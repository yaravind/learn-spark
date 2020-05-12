package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.loadSales
import org.apache.spark.sql.{DataFrame, SparkSession}

/** *
 * Problem 3: How many distinct products have been sold in each day?
 */
object HowManyDistinctProductsHaveBeenSoldInEachDayApp extends App with Logging {
  val spark = getSparkSession("HowManyDistinctProductsHaveBeenSoldInEachDayApp", getClusterCfg(args))

  val orderDF = loadSales()

  val result = run(spark)
  result.show(10)

  def run(spark: SparkSession): DataFrame = {
    var sqlText =
      """ SELECT date, count(DISTINCT product_id) as sold_cnt
        | FROM orders
        | GROUP BY date
        | ORDER BY sold_cnt DESC
        |""".stripMargin
    val result = spark.sql(sqlText)

    result
  }

}
