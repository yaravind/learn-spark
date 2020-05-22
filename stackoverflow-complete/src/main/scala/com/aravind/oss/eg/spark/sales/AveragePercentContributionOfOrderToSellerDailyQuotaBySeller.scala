package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.{loadSales, loadSellers}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>For each seller, what is the average % contribution of an order to the seller's daily quota?</p>
 *
 * <p><b>Example</b></p>
 *
 * <p>
 * If Seller_0 has a daily_target of 250 and has 3 orders (order_count):
 * <ol>
 * <li>Order 1: 10 num_pieces_sold</li>
 * <li>Order 2: 8 num_pieces_sold</li>
 * <li>Order 3: 7 num_pieces_sold</li>
 * </ol>
 *
 * The average % contribution of orders to the seller's quota would be: sum(num_pieces_sold/daily_target)/order_count
 *
 * <ol>
 * <li>Order 1: 10/250 = 0.004</li>
 * <li>Order 2: 8/250 = 0.032</li>
 * <li>Order 3: 7/250 = 0.028</li>
 * </ol>
 * Average % Contribution = sum(num_pieces_sold/daily_target) / order_count = (0.004 + 0.032 + 0.028)/3 = 0.064/3 = 0.02133
 * </p>
 */
object AveragePercentContributionOfOrderToSellerDailyQuotaBySeller extends App with Logging {
  val spark = getSparkSession("AveragePercentContributionOfOrderToSellerDailyQuotaBySeller", getClusterCfg(args))

  val orderDF = loadSales()
  val sellerDF = loadSellers()
  logInfo("Q: For each seller, what is the average % contribution of an order to the seller's daily quota?")
  val result = run(spark)
  logInfo("Answer:")
  result.show()

  def run(spark: SparkSession): DataFrame = {
    // Not needed (as Spark auto broadcasts it because spark.sql.autoBroadcastJoinThreshold by default set to  10M)
    // but we can force the broadcast using /*+ BROADCAST(sellers) */ hint
    val q =
    """
      | SELECT
	    |	  o.seller_id,
	    |	  avg(o.num_pieces_sold/s.daily_target) AS percent_contribution
      | FROM orders o
	    | JOIN sellers s ON o.seller_id = s.seller_id
      | GROUP BY o.seller_id
      | """.stripMargin

    spark.sql(q)
  }
}
