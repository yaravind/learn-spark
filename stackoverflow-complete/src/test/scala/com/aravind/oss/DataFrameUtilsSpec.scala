package com.aravind.oss

import com.aravind.oss.eg.spark.SparkAppUtil

import java.time.LocalDateTime

class DataFrameUtilsSpec extends BaseSpec with DataFrameUtils {
  "A Dataset" should "equal itself" in {
    val spark = SparkAppUtil.getSparkSession("DataFrameUtilsSpec", "local[1]")
    val allFiles = filesBetween("datasets/rendata/refined/master/naouplanRe/out/workdir/tmpRecord", LocalDateTime.now(), spark)
    println(s"Total files: ${allFiles.length}")
    allFiles.foreach(f => {println(f)})
    "a" == "a"
  }
}
