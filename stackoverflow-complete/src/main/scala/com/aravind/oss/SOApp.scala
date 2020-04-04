package com.aravind.oss

import org.apache.spark.sql.SparkSession

object SOApp extends App with Logging {

  logInfo("""Hello World!""")

  val spark = SparkSession
    .builder()
    .appName("Stackoverflow App")
    .master("local[*]")
    .getOrCreate()

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

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

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