package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil._
import org.apache.spark.sql.{Row, SparkSession}
import ProductSalesUtil._

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

  //--- Problem 1
  logInfo("Q: How many products have been sold at least once?")
  val count = howManyProductsHaveBeenSoldAtLeastOnce(spark)
  logInfo("A (should be 993429): " + count)

  //--- Problem 2
  logInfo("Q: Which is the product contained in more orders?")
  val product_id = whichIsTheProductContainedInMoreOrders(spark)
  logInfo("A (should be product_id '0', which is sold 19000000 times): " + product_id)

  def howManyProductsHaveBeenSoldAtLeastOnce(spark: SparkSession): Long = {
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

  //--- Problem 3
  logInfo("Q: How many distinct products have been sold in each day?")
  var sqlText =
    """ SELECT date, count(DISTINCT product_id) as sold_cnt
      | FROM orders
      | GROUP BY date
      | ORDER BY sold_cnt DESC
      |""".stripMargin

  spark.sql(sqlText).show(10)

  //--Problem 4
  logInfo("Q: What is the average revenue of the orders?")
  /**
   * total_revenue = sum(num_pieces_sold * unit_price)
   * avg_revenue = total_revenue / orders_count
   */
  sqlText =
    """ SELECT avg(p.price * o.num_pieces_sold) as total_revenue
      | FROM orders o
      | JOIN PRODUCTS p ON p.product_id = o.product_id
      |""".stripMargin
  val totalRevenue = spark.sql(sqlText).show(15)
  /*.select("total_revenue")
  .collect()(0) //first row
  .getDouble(0)*/

  /*logInfo("Total Revenue: " + totalRevenue)

  val ordersCount = orderDF.count()

  logInfo("A: " + (totalRevenue / ordersCount))*/

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
