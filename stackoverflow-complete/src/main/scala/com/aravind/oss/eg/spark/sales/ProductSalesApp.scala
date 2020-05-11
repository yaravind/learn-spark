package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import com.aravind.oss.eg.spark.sales.ProductSalesUtil._
import org.apache.spark.sql.Row

/** *
 * Solution for https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
 */
object ProductSalesApp extends App with Logging {

  val spark = getSparkSession("ProductSalesApp", getClusterCfg(args))

  logInfo("Meta info - Products")
  val productDF = loadProducts()
  productDF.show(5)

  logInfo("Meta info - Sellers")
  val sellerDF = loadSellers()
  sellerDF.show(5)

  logInfo("Meta info - Orders")
  val orderDF = loadSales()
  orderDF.show(5)

  logInfo("Count - Products: " + productDF.count())
  logInfo("Count - Sellers: " + sellerDF.count())
  logInfo("Count - Orders: " + orderDF.count())

  //Problem 4 Step 1 - Check and select the skewed keys

  val skewedSqlText =
    """ SELECT count(product_id) AS cnt
      | FROM products
      | GROUP BY product_id
      | ORDER BY cnt DESC
      |""".stripMargin


  val skewedData: Array[Row] = spark.sql(skewedSqlText).limit(1).collect()


  /**
   * Problem 4 Step 2 - What we want to do is:
   *  a. Duplicate the entries that we have in the dimension (PRODUCTS) table for the most common prod
   * product_0 will become: product_0-1, product_0-2, product_0-3 and so on
   * b. On the sales table, we are going to replace "product_0" with a random duplicate (e
   * will be replaced with product_0-1, others with product_0-2, etc.)
   * Using the new "salted" key will unskew the join
   */
  val ReplicationFactor = 5
  val duplicates = for {
    _r <- skewedData
    _i <- 0 until ReplicationFactor
  } yield (_r.getString(0), (_r.getString(0), _i.toString))

  //val a = duplicates.map(tuple => tuple._1)
  val l = duplicates.map(tuple => tuple._2)


  val replicatedDF = spark.createDataFrame(
    spark.sparkContext.parallelize(l)
  ).toDF("product_id", "replication")
  replicatedDF.printSchema()
  replicatedDF.show(2)


}
