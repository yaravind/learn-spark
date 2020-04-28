package com.aravind.oss.eg.spark.sales

import com.aravind.oss.{Logging, SparkSessionTestWrapper}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.FlatSpec

class ProductSalesAppTestSpec extends FlatSpec with SparkSessionTestWrapper with DatasetComparer with Logging {

  val orderCols = Seq("order_id", "product_id", "seller_id", "date", "num_pieces_sold", "bill_raw_text")

  "howManyProductsHaveBeenSoldAtLeastOnce" should "return zero when no orders are made for products" in {
    logInfo("START isStopped: " + spark.sparkContext.isStopped)
    logInfo("START isLocal: " + spark.sparkContext.isLocal)
    import spark.implicits._

    val productCols = Seq("product_id", "product_name", "price")
    val productDF = Seq(
      ("0", "product_0", "22"),
      ("1", "product_1", "30"),
      ("2", "product_2", "91")
    ).toDF(productCols: _*)
    productDF.createOrReplaceTempView("PRODUCTS")

    val orderDF = Seq(
      ("1", "10", "0", "2020-07-10", "26", "kyeibuumwlyhuwksx"),
      ("2", "20", "0", "2020-07-08", "13", "kyeibuumwlyhuwksx"),
      ("3", "30", "0", "2020-07-05", "38", "kyeibuumwlyhuwksx"),
      ("4", "40", "0", "2020-07-05", "56", "kyeibuumwlyhuwksx")
    ).toDF(orderCols: _*)
    orderDF.createOrReplaceTempView("ORDERS")

    assert(ProductSalesApp.howManyProductsHaveBeenAoldAtLeastOnce(spark) == 0)
  }

  "howManyProductsHaveBeenSoldAtLeastOnce" should "return 1" in {
    logInfo("START isStopped: " + spark.sparkContext.isStopped)
    logInfo("START isLocal: " + spark.sparkContext.isLocal)

    import spark.implicits._
    val orderDF = Seq(
      ("1", "0", "0", "2020-07-10", "26", "kyeibuumwlyhuwksx"),
      ("2", "20", "0", "2020-07-08", "13", "kyeibuumwlyhuwksx"),
      ("3", "30", "0", "2020-07-05", "38", "kyeibuumwlyhuwksx"),
      ("4", "40", "0", "2020-07-05", "56", "kyeibuumwlyhuwksx")
    ).toDF(orderCols: _*)
    orderDF.createOrReplaceTempView("ORDERS")
    assert(ProductSalesApp.howManyProductsHaveBeenAoldAtLeastOnce(spark) == 1)
  }

  "howManyProductsHaveBeenSoldAtLeastOnce" should "return 1 for products sold more tha once" in {
    logInfo("END isStopped: " + spark.sparkContext.isStopped)
    logInfo("END isLocal: " + spark.sparkContext.isLocal)
    import spark.implicits._
    val orderDF = Seq(
      ("1", "0", "0", "2020-07-10", "26", "kyeibuumwlyhuwksx"),
      ("2", "0", "0", "2020-07-08", "13", "kyeibuumwlyhuwksx"),
      ("3", "30", "0", "2020-07-05", "38", "kyeibuumwlyhuwksx"),
      ("4", "40", "0", "2020-07-05", "56", "kyeibuumwlyhuwksx")
    ).toDF(orderCols: _*)
    orderDF.createOrReplaceTempView("ORDERS")
    assert(ProductSalesApp.howManyProductsHaveBeenAoldAtLeastOnce(spark) == 1)
  }

  "howManyProductsHaveBeenSoldAtLeastOnce" should "return 3" in {
    logInfo("END isStopped: " + spark.sparkContext.isStopped)
    logInfo("END isLocal: " + spark.sparkContext.isLocal)
    import spark.implicits._
    val orderDF = Seq(
      ("1", "0", "0", "2020-07-10", "26", "kyeibuumwlyhuwksx"),
      ("2", "0", "0", "2020-07-08", "13", "kyeibuumwlyhuwksx"),
      ("3", "1", "0", "2020-07-05", "38", "kyeibuumwlyhuwksx"),
      ("4", "2", "0", "2020-07-05", "56", "kyeibuumwlyhuwksx")
    ).toDF(orderCols: _*)
    orderDF.createOrReplaceTempView("ORDERS")
    assert(ProductSalesApp.howManyProductsHaveBeenAoldAtLeastOnce(spark) == 3)
  }
}
