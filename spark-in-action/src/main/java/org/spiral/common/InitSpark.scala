package org.spiral.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 初始化SparkConf
  *
  * @author dengguoqing
  * @since 2018-11-06
  */
class InitSpark(val master: String = "local[4]", val appName: String = "My App") {
  def sparkConf() = {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    sc
  }

  def SparkSessionBuilt(): SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .config("configuration_key", "configuration_value")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession
  }

  def sparkSessionInit(appName: String = "default",
                       master: String = "local"): SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("configuration_key", "configuration_value")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession
  }
}

object InitSpark {
  val initSpark = new InitSpark

  def getSparkConf() = {
    initSpark.sparkConf()
  }

  def defaultSparkSession(appName: String = "default",
                          master: String = "local"): SparkSession = {
    initSpark.sparkSessionInit(appName, master)
  }

}
