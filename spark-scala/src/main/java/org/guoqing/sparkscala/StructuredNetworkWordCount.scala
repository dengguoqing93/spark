package org.guoqing.sparkscala

import org.guoqing.sparkscala.config.InitSpark

/**
  * sparkStream测试套接字连接
  *
  * nc -lk 9999
  *
  * @author DGQ
  * @since 2019/3/11
  */
object StructuredNetworkWordCount {
  def main(args: Array[String]): Unit = {
    val spark = InitSpark.defaultSparkSession()
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    import spark.implicits._
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
