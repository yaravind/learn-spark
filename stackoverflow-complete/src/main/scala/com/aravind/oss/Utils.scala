package com.aravind.oss

import com.microsoft.spark.notebook.msutils.MSFileInfo
import org.apache.spark.sql.{DataFrame, SparkSession}

/**   *
 * Utility methods to work with Synapse environment.
 */
object Utils {

  /**
   * List all files and folders in specified path and sub-folders recursively. The out-of-the-box
   * [[mssparkutils.fs.ls]] isn't recursive.
   *
   * @param root initial directory to start the listing from
   * @return an [[Array]] of [[MSFileInfo]] objects
   */
  def deepLs(root: MSFileInfo): Array[MSFileInfo] = {
    val these = mssparkutils.fs.ls(root.path)
    these ++ these.filter(_.isDir).flatMap(deepLs)
  }

  /**
   * Collects all files and folders of the given path and sub-folders recursively and creates a Spark DataFrame to
   * enable querying and pretty print using Notebook's '''display()''' method.
   *
   * @param rootStr initial directory to start the listing from
   *
   * @param spark   an instance of [[SparkSession]] used to create a [[DataFrame]]
   * @return a Spark [[DataFrame]] with columns '''name, path, size, isDir, isFile'''
   *
   * @example {{{val rootStr = "abfss://container@storage-account.net/data/v1"
   *val fileDf = toDataFrame(rootStr, spark)}}}
   */
  def deepLsToDataFrame(rootStr: String, spark: SparkSession): DataFrame = {
    val root = MSFileInfo(name = null, path = rootStr, size = 0, isDir = true, isFile = false)
    val files = deepLs(root)

    // sc.parallelize() method creates a distributed and parallelized dataset on existing collection on the "driver"
    val fileDf = spark.createDataFrame(spark.sparkContext.parallelize(files))
    return fileDf
  }

  /**
   * @see https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc
   * @see https://en.wikipedia.org/wiki/Zettabyte
   *
   * @param fileSize Up to Exabytes
   * @return
   */
  def humanReadableByteSize(fileSize: Long): String = {
    if (fileSize <= 0) return "0 B"
    // kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta
    val units: Array[String] = Array("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    val digitGroup: Int = (Math.log10(fileSize.toDouble) / Math.log10(1024)).toInt
    f"${fileSize / Math.pow(1024, digitGroup.toDouble)}%3.3f ${units(digitGroup)}"
  }
}
