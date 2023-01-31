package com.aravind.oss

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest._

abstract class BaseSpec extends FlatSpec
  with DataFrameSuiteBase with DatasetComparer
  with Matchers with OptionValues with Inside with Inspectors {

}
