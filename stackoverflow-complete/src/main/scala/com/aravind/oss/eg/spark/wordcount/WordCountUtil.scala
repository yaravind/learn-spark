package com.aravind.oss.eg.spark.wordcount

object WordCountUtil {
  //matches a space, a tab, a carriage return, a line feed, or a form feed
  val WhitespaceRegex = "[\\s]"

  def getPaths(args: Array[String]): Seq[String] = {
    val paths: Seq[String] = if (!args.isEmpty) {
      args(0)
        .split(",")
        .map(x => x.trim)
        .filter(x => !x.isEmpty)
    }
    else {
      Seq("src/main/resources/wordcount/test.txt")
    }

    paths
  }
}
