package org.guoqing.sparkscala

import org.apache.spark.sql.types.StructType
import org.guoqing.sparkscala.config.InitSpark

/**
  * 从csv文件获取数据流
  *
  * @author DGQ
  * @since 2019/3/11
  */
object SparkSteamCsv {
  def main(args: Array[String]): Unit = {
    val spark = InitSpark.defaultSparkSession()
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("/Users/dengguoqing/IdeaProjects/spark/test.csv") // Equivalent to format("csv").load("/path/to/directory")
    println(csvDF.isStreaming)
    csvDF.printSchema()
  }
}
