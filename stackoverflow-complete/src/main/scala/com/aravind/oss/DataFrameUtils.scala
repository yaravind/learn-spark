package com.aravind.oss

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import org.apache.spark.sql.types._

import java.time.LocalDateTime
import scala.collection.JavaConversions.asScalaBuffer
import scala.reflect.runtime.universe

trait DataFrameUtils {

  @transient private var log: Logger = LogManager.getLogger(getClass.getName.stripSuffix("$"))

  /** *
   * Get the JSON version of the schema of given [[DataFrame]].
   */
  def getSchemaAsJson(df: DataFrame): String = {
    df.schema.json
  }

  def addFilePathColumn(df: DataFrame)(spark: SparkSession): DataFrame = {
    spark.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)
    df.withColumn("file_path", input_file_name())
    //df.withColumn("fileName", callUDF("get_file_name", input_file_name())).show()
  }

  def addFileNameCol(df: DataFrame)(spark: SparkSession): DataFrame = {
    df.withColumn("file_name",
      regexp_extract(input_file_name, """[\/]([^\/]+[\/][^\/]+)$""", 1))
  }


  /**
   * Create a schema from json
   *
   * @param jsonStr json version of serialized StructType
   * @return [[StructType]]
   */
  def fromJsonToSchema(jsonStr: String): StructType = {
    DataType.fromJson(jsonStr).asInstanceOf[StructType]
  }

  def getSchemaAsDictionary(df: DataFrame): StructType = {
    df.schema
  }

  /**
   * Sometimes the data being read has spaces in the column names and mixed case alphabet etc. This function does the
   * following in that order.
   *
   *  1. trims leading and trailing whitespaces
   *  1. removes hyphens.
   *  1. removes forward slashes.
   *  1. removes periods.
   *  1. replaces spaces with '_'.
   *  1. changes to lowercase.
   *
   * @return new [[DataFrame]] with new column names
   */
  def cleanColumnNames(df: DataFrame): DataFrame = {
    val oldCols = df.columns
    val renamedColumns: Array[String] = oldCols.map(oldColName => cleanName(oldColName))

    val oldVsNew = oldCols.zip(renamedColumns)
    if (log.isInfoEnabled) {
      oldVsNew.foreach(x => log.info(s"(${x._1}, ${x._2})"))
    }

    val renamedDf = df.toDF(renamedColumns: _*)
    renamedDf
  }

  def cleanName(oldName: String) = {
    oldName
      .trim() //Remove leading and trailing whitespaces
      .replaceAll("-", " ") // Replace hyphen used as a separator
      .replaceAll("\\/", " ") // Replace / used as a separator
      .replaceAll("[()]", " ") // Replace left  and right parenthesis used as a separator
      .replaceAll("\\.", " ") // Dot notation is used to fetch values from fields that are nested
      .replaceAll("\\s+", " ") // Reduce multiple consecutive whitespaces to one
      .replaceAll(" ", "_") // Replace all single whitespace with underscores
      .toLowerCase()
  }

  def dropColumns(df: DataFrame, cols: List[String]): DataFrame = {
    // function to drop spark dataframe columns in bulks, which is available after spark2.x
    df.select(df.columns.diff(cols).map(x => new Column(x)): _*)
  }

  def typeMatch(input: String): DataType = {
    import org.apache.spark.sql.types._
    input match {
      case "Int" => IntegerType
      case "String" => StringType
      case "Date" => DateType
      case "Double" => DoubleType
      case "Timestamp" => TimestampType
    }
  }

  def changeColType(df: DataFrame, col: String, newType: String): DataFrame = {
    df.withColumn(col, df(col).cast(typeMatch(newType)))
  }

  def changeMulColType(df: DataFrame, colName: List[String], newType: List[String]): DataFrame = {
    val types = if (newType.length == 1) List.fill(colName.length)(newType.head) else newType
    colName.zip(types).foldLeft(df) {
      (table, zipped_col_type) => changeColType(table, zipped_col_type._1, zipped_col_type._2)
    }
  }

  def changeAllColType(df: DataFrame, sourceType: String, newType: String): DataFrame = {
    df.schema.filter(_.dataType == typeMatch(sourceType)).foldLeft(df) {
      // keyword case is optional
      case (table, col) => changeColType(table, col.name, newType)
    }
  }

  import scala.reflect.runtime.universe._

  /** *
   * Get the type information at runtime.
   *
   * @param value variable (val) for which you want the type info
   *
   * @tparam T generic type
   * @return a [[Type]] object
   *
   * @example {{{
   *          val a = Seq("1", "3")
   *          getType(a)
   * }}}
   */
  def getType[T: TypeTag](value: T): universe.Type = typeOf[T]

  /**
   * Returns a [[DataFrame]] with two columns: partition_id and record_count
   */
  def getRecordsPerPartition(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.spark_partition_id
    df.withColumn("partition_id", spark_partition_id)
      .groupBy("partition_id")
      .count
      .withColumnRenamed("count", "record_count")
  }

  /** *
   * scanning the last two hours of folders and then taking the most recent CSV files and generating a single list.
   * If both the hours folders contain files, the code below is working as expected. but if any folder does not contain
   * any files, then it is showing "ArrayIndexOutOfBoundsException: 0"
   *
   * @param spark
   */
  def filesBetween(rootStr: String, endTimestamp: LocalDateTime = LocalDateTime.now, spark: SparkSession): List[String] = {
    import scala.language.postfixOps
    val hdfsConf = new Configuration();
    //var rootStr = "/user/hdfs/test/input"
    var finalFiles = List[String]()
    //val endTimestamp = LocalDateTime.now
    /*val hours = 2
    var paths = (0 until hours.toInt)
      .map(h => {
        println(h)
        endTimestamp.minusHours(h)
      })
      .map(ts => s"${rootStr}/partition_date=${ts.toLocalDate}/hour=${ts.toString.substring(11, 13)}")
      .toList*/

    /*
     The above outputs the following
     paths: List(/user/hdfs/test/input/partition_date=2022-11-30/hour=19,
      /user/hdfs/test/input/partition_date=2022-11-30/hour=18)
    */
    /*
    Need to explicitly import encoders to avoid the following error

    Unable to find encoder for type Array[String]. An implicit Encoder[Array[String]] is
    needed to store Array[String] instances in a Dataset.
     */
    import spark.implicits._
    //println(s"paths: $paths")
    for (eachfolder <- List(rootStr)) { //paths
      var New_Folder_Path: String = eachfolder
      var fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      var pathstatus = fs.listStatus(new Path(New_Folder_Path))
      var currpathfiles = pathstatus.map(x => Row(x.getPath.toString, x.getModificationTime))
      currpathfiles.foreach(row => {
        println(row.getString(0), row.getLong(1))
      })
      println(s"currpathfiles: ${currpathfiles}")
      var latestFile = spark.sparkContext.parallelize(currpathfiles)
        .map(row => (row.getString(0), row.getLong(1)))
        .toDF("FilePath", "ModificationTime")
        //.filter(col("FilePath").like("%.csv%"))
        .sort($"ModificationTime".desc)
        .select(col("FilePath"))
        .map(row => row.getString(0))
        .collectAsList
        .headOption //handle empty folder with no files by avoiding ArrayIndexOutOfBoundsEx
        .foreach(latestFile => finalFiles = latestFile :: finalFiles)
    }
    finalFiles
  }
}