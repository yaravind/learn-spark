package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.loadSales
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Problem: Who are the second most selling and the least selling persons (sellers) for each product?
 * Who are those for product with `product_id = 0`
 */
object SecondMostSellingAndLeastSellingSellersForEachProductApp extends App with Logging {
  val spark = getSparkSession("SecondMostSellingAndLeastSellingSellersForEachProductApp", getClusterCfg(args))

  val result = run(spark)

  logInfo("Who are the second most selling and the least selling persons (sellers) for each product_id '0'?")
  logInfo("A: ")
  result
    .where(col("product_id") === "0")
    .show()

  def run(spark: SparkSession): DataFrame = {
    val orderDF = loadSales()

    //1. We need two new ranking columns: one that ranks the products’ sales in
    // descending order and another one that ranks in ascending order.
    val windowSpecDesc = Window.partitionBy("product_id").orderBy(col("tot_num_pieces_sold").desc)
    val windowSpecAsc = Window.partitionBy("product_id").orderBy(col("tot_num_pieces_sold").asc)

    //2. We get the sum of sales for each product and seller pairs.
    val totSalesByProdAndSeller = orderDF
      .groupBy(col("product_id"), col("seller_id"))
      .agg(sum(col("num_pieces_sold")).alias("tot_num_pieces_sold"))
      .withColumn("rank_asc", dense_rank().over(windowSpecAsc))
      .withColumn("rank_desc", dense_rank().over(windowSpecDesc))
      .cache() //Cache for reuse
    logInfo("Total sum of sales grouped by product and seller:")
    totSalesByProdAndSeller.show()
    //totSalesByProdAndSeller.where(col("rank_desc").notEqual("1")).show()
    //totSalesByProdAndSeller.where(col("rank_asc").notEqual("1")).show()

    //3. We split the dataset obtained in three pieces: one for each case that we want to handle
    // (second top selling, least selling, single selling).

    //3.1: Single selling
    //
    // If a product has been sold by only one seller, we’ll put it into a special category
    // (category: Only seller or multiple sellers with the same quantity).
    //If a product has been sold by more than one seller, but all of them sold the same quantity,
    // we are going to put them in the same category as if they were only a single seller for that
    // product (category: Only seller or multiple sellers with the same quantity).
    val singleSeller = totSalesByProdAndSeller
      .where(col("rank_asc") === col("rank_desc"))
      .select(
        col("product_id").alias("single_seller_product_id"),
        col("seller_id").alias("single_seller_seller_id"),
        lit("Only seller or multiple sellers with the same results").alias("type"))
    logInfo("Single seller:")
    singleSeller.show()

    //3.2: Second top selling Get the second top sellers
    //
    // If the “least selling” is also the “second selling”, we will count it only as “second seller”
    val secondSeller = totSalesByProdAndSeller
      .where(col("rank_desc") === 2)
      .select(
        col("product_id").alias("second_seller_product_id"),
        col("seller_id").alias("second_seller_seller_id"),
        lit("Second top seller").alias("type")
      )
    logInfo("Second top seller:")
    secondSeller.show()

    //3.3: Least sellers
    //
    // Get the least sellers and exclude those rows that are already included in the first piece
    // We also exclude the "second top sellers" that are also "least sellers"
    val leastSeller = totSalesByProdAndSeller
      .where(col("rank_asc") === 1)
      .select(
        col("product_id"),
        col("seller_id"),
        lit("Least seller").as("type")
      )
      .join( //exclude single seller
        singleSeller,
        (totSalesByProdAndSeller("seller_id") === singleSeller("single_seller_seller_id")) &&
          (totSalesByProdAndSeller("product_id") === singleSeller("single_seller_product_id")),
        "left_anti"
      )
      .join( //exclude second top seller
        secondSeller,
        (totSalesByProdAndSeller("seller_id") === secondSeller("second_seller_seller_id")) &&
          (totSalesByProdAndSeller("product_id") === secondSeller("second_seller_product_id")),
        "left_anti"
      )
    logInfo("Least seller:")
    leastSeller.show()

    //4. Union all the results
    val result = leastSeller.select(
      col("product_id"),
      col("seller_id"),
      col("type")
    ).union(
      secondSeller.select(
        col("second_seller_product_id").alias("product_id"),
        col("second_seller_seller_id").alias("seller_id"),
        col("type")
      )
    ).union(
      singleSeller.select(
        col("single_seller_product_id").alias("product_id"),
        col("single_seller_seller_id").alias("seller_id"),
        col("type")
      )
    )
    result
  }

}
