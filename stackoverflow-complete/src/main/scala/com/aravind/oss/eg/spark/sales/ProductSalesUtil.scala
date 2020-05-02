package com.aravind.oss.eg.spark.sales

import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesApp.args
import org.apache.spark.sql.DataFrame

object ProductSalesUtil {
  type TableName = String
  val SalesRoot = "/Users/o60774/Downloads/product-sales"

  def loadProducts(): DataFrame = {
    createAndRegister(SalesRoot + "/products_parquet/*.parquet", "PRODUCTS")
  }

  def loadSales(): DataFrame = {
    createAndRegister(SalesRoot + "/sales_parquet/*.parquet", "ORDERS")
  }

  def loadSellers(): DataFrame = {
    createAndRegister(SalesRoot + "/sellers_parquet/*.parquet", "SELLERS")
  }

  def createAndRegister(path: String, name: TableName): DataFrame = {
    val spark = getSparkSession("ProductSalesApp", getClusterCfg(Array()))
    val DF = spark.read.parquet(path)
    DF.createOrReplaceTempView(name)
    DF.printSchema()

    DF
  }
}
