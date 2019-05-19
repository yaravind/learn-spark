package com.aravind.oss

import org.apache.spark.sql.SparkSession

object Apollo extends App with Logging {
  logInfo("""Hello World!""")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

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
  // root
  // |-- age: long (nullable = true)
  // |-- name: string (nullable = true)

  // Select only the "name" column
  df.select("name").show()
  // +-------+
  // |   name|
  // +-------+
  // |Michael|
  // |   Andy|
  // | Justin|
  // +-------+

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
  // +---+----+
  // |age|name|
  // +---+----+
  // | 30|Andy|
  // +---+----+

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
