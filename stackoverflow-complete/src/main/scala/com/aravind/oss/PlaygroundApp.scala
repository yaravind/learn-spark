package com.aravind.oss

import com.aravind.oss.eg.spark.SparkAppUtil
import org.apache.spark.sql.SparkSession

object PlaygroundApp extends App with Logging {
  val spark = SparkSession
    .builder()
    .appName("Stackoverflow App")
    .master("local[*]")
    .getOrCreate()

  val executors = SparkAppUtil.activeExecutors(spark)
  //  val df = spark.read.json("src/main/resources/people.json")
  //  df.printSchema()

  logInfo("Executors count: " + executors.size)
  executors.foreach("Executor: " + println(_))

  val creditsDF = spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .load("/Users/o60774/Downloads/credits.csv")

  creditsDF.printSchema()

  creditsDF.show(3)

  logInfo("Total: " + creditsDF.count())

  /*
   val sourceDf: Dataset[Row] = Seq(
     ("6", "1", "hi"),
     ("4", "5", "bye"),
     ("8", "9", "night")
   ).toDF("A", "B", "C")

   val sqlTxt1 =
     """ SELECT key1, count(*)
       | FROM (
       |       SELECT split(fact.salted_key1,"-")[0] AS key1
       |       FROM orders_salted fact, products_salted dim
       |       WHERE fact.salted_key1 = dim.salted_product_id
       | )
       | GROUP BY key1
       | ORDER BY key1
       |""".stripMargin

   val res = spark.sql("select fact.salted_key1, count(*) " +
     "from ORDERS_SALTED fact, PRODUCTS_SALTED dim where fact.salted_key1 = dim.salted_product_id " +
     "group by fact.salted_key1 order by fact.salted_key1")
   // For implicit conversions like converting RDDs to DataFrames

   import spark.implicits._

  val nums = Seq(
      (1, 2),
      (4, 4),
      (0, 3)
    ).toDF("col1", "col2")

    val sumDF = nums
     .withColumn("addition", nums("col1") + nums("col2"))
   sumDF.show()

   //  +----+----+--------+
   //  |col1|col2|addition|
   //  +----+----+--------+
   //  |   1|   2|       3|
   //  |   4|   4|       8|
   //  |   0|   3|       3|
   //  +----+----+--------+
   sumDF
     .withColumn("alert", sumDF("addition") > 3)
     .show()
   //  +----+----+--------+-----+
   //  |col1|col2|addition|alert|
   //  +----+----+--------+-----+
   //  |   1|   2|       3|false|
   //  |   4|   4|       8| true|
   //  |   0|   3|       3|false|
   //  +----+----+--------+-----+*/
}