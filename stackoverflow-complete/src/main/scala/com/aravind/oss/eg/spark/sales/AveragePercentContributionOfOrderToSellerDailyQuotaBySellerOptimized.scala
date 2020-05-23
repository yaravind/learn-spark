package com.aravind.oss.eg.spark.sales

import com.aravind.oss.Logging
import com.aravind.oss.eg.spark.SparkAppUtil.{getClusterCfg, getSparkSession}
import com.aravind.oss.eg.spark.sales.ProductSalesUtil.{loadSales, loadSellers}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <p>For each seller, what is the average % contribution of an order to the seller's daily quota?</p>
 * See [[AveragePercentContributionOfOrderToSellerDailyQuotaBySeller]] for <b>unoptimized</b> version.
 * <p><b>Example</b></p>
 */
object AveragePercentContributionOfOrderToSellerDailyQuotaBySellerOptimized extends App with Logging {
  val spark = getSparkSession("AveragePercentContributionOfOrderToSellerDailyQuotaBySeller", getClusterCfg(args))

  // Change 1: ********* Reduce the shuffle partitions to 1 as there are lot of empty partitions being created

  /*
    The exact logic for coming up with number of shuffle partitions depends on actual
    analysis. You can typically set it to be 1.5 or 2 times of the initial partitions.
    */
  spark.conf.set("spark.sql.shuffle.partitions", 1)

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
