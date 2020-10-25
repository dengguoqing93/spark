package org.guoqing.sparkscala

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.guoqing.sparkscala.config.InitSpark


/**
  * sparkSteam 基本操作
  *
  * @author DGQ
  * @since 2019/3/11
  */
case class DeviceData(device: String, deviceType: String, signal: Double, time: String)

object BasicOperation {
  def main(args: Array[String]): Unit = {
    val spark = InitSpark.defaultSparkSession()
    import spark.implicits._
    val struct = new StructType().add("device", "string").add("deviceType", "string")
      .add("signal", "double").add("time", "string")
    val df: DataFrame = spark.readStream.schema(struct)
      .csv("/Users/dengguoqing/IdeaProjects/spark/test.csv")
    val ds: Dataset[DeviceData] = df.as[DeviceData]
    // Select the devices which have signal more than 10
    df.select("device").where("signal > 10") // using untyped APIs
    ds.filter(_.signal > 10).map(_.device) // using typed APIs

    // Running count of the number of updates for each device type
    df.groupBy("deviceType").count() // using untyped API

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal)) // using typed API
    df.createOrReplaceTempView("updates")
    spark.sql("select count(*) from updates") // returns another streaming DF
    df.isStreaming
  }
}
