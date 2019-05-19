package com.ncr.eda.apollo

import com.holdenkarau.spark.testing.DatasetSuiteBase

class SimpleDataSetSpec extends BaseSpec with DatasetSuiteBase {
  override implicit def enableHiveSupport: Boolean = false

  "A Dataset" should "equal itself" in {
    import sqlContext.implicits._

    val list = List(Person("Naruto", 2000, 60.0), Person("Sasuke", 23, 80.0))
    val persons = sc.parallelize(list).toDS

    assertDatasetEquals(persons, persons)
  }
}

case class Person(name: String, age: Int, weight: Double)
