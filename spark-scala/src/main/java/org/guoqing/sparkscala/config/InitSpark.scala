package org.guoqing.sparkscala.config

import org.apache.spark.{SparkConf, SparkContext}

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @since 2018-11-06
  */
class InitSpark(val master: String = "local", val appName: String = "My App") {
  def sparkConf() = {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf)
    sc
  }
}

object InitSpark {
  val spark = new InitSpark

  def sparkConf() = {
    spark.sparkConf()
  }
}
