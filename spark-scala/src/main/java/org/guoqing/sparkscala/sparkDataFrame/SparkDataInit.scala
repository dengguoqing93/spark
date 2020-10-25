package org.guoqing.sparkscala.sparkDataFrame

import org.apache.spark.sql.SparkSession

/**
  * $DESCRIPTION
  *
  * @author DGQ
  * @since 2018/12/24
  */
class SparkDataInit {

}

object SparkDataInit {
  def init(): SparkSession = {
    val sparkSession = SparkSession.builder().appName("test")
      .config("configuration_key", "configuration_value")
      .enableHiveSupport().getOrCreate()
    sparkSession
  }
}
