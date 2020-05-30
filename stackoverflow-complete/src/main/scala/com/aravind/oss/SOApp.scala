package com.aravind.oss

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object SOApp extends App with Logging {

  logInfo("""Hello World!""")

  val spark = SparkSession
    .builder()
    .appName("Stackoverflow App")
    .master("local[*]")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._

  import org.apache.spark.sql.functions._
  val input = Seq(
    (1, 1, 5),
    (1, 2, 5),
    (1, 3, 5),
    (2, 1, 15),
    (2, 2, 5),
    (2, 6, 5)
  ).toDF("id", "col1", "col2")

  val result = input
    .groupBy("id")
    .agg(max(col("col1")),sum(col("col2")))
    .show()

  val df = spark.read.json("src/main/resources/people.json")
  // Displays the content of the DataFrame to stdout
  df.show()
  // +----+-------+
  // | age|   name|
  // +----+-------+
  // |null|Michael|
  // |  30|   Andy|
  // |  19| Justin|
  // +----+-------+

  // Print the schema in a tree format
  df.printSchema()


  // Select only the "name" column
  logInfo("""Select only the "name" column""")
  df.select("name").show()

  // Select everybody, but increment the age by 1
  df.select($"name", $"age" + 1).show()
  // +-------+---------+
  // |   name|(age + 1)|
  // +-------+---------+
  // |Michael|     null|
  // |   Andy|       31|
  // | Justin|       20|
  // +-------+---------+

  // Select people older than 21
  df.filter($"age" > 21).show()

  // Count people by age
  df.groupBy("age").count().show()
  // +----+-----+
  // | age|count|
  // +----+-----+
  // |  19|    1|
  // |null|    1|
  // |  30|    1|
  // +----+-----+
}