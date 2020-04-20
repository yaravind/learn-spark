package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import org.apache.spark.sql.SparkSession

/** *
 * Solution for https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
 */
object ProductSalesApp extends App with Logging {
  val SalesRoot = "/Users/o60774/Downloads/product-sales"
  val spark = getSparkSession("ProductSalesApp", getClusterCfg(args))

  logInfo("Meta info - Products")
  val productDF = spark.read.parquet(SalesRoot + "/products_parquet/*.parquet")
  productDF.createOrReplaceTempView("PRODUCTS")
  productDF.printSchema()
  productDF.show(5)

  logInfo("Meta info - Sellers")
  val sellerDF = spark.read.parquet(SalesRoot + "/sellers_parquet/*.parquet")
  sellerDF.createOrReplaceTempView("SELLERS")
  sellerDF.printSchema()
  sellerDF.show(5)

  logInfo("Meta info - Orders")
  val orderDF = spark.read.parquet(SalesRoot + "/sales_parquet/*.parquet")
  orderDF.createOrReplaceTempView("ORDERS")
  orderDF.printSchema()
  orderDF.show(5)

  logInfo("Count - Products: " + productDF.count())
  logInfo("Count - Sellers: " + sellerDF.count())
  logInfo("Count - Orders: " + orderDF.count())

  //--- Problem 1
  logInfo("Q: How many products have been sold at least once?")
  val count = howManyProductsHaveBeenAoldAtLeastOnce(spark)
  logInfo("A (should be 993429): " + count)

  //--- Problem 2
  logInfo("Q: Which is the product contained in more orders?")
  val product_id = whichIsTheProductContainedInMoreOrders(spark)
  logInfo("A (should be product_id '0', which is sold 19000000 times): " + product_id)

  def howManyProductsHaveBeenAoldAtLeastOnce(spark: SparkSession): Long = {
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

  def whichIsTheProductContainedInMoreOrders(spark: SparkSession): String = {
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
