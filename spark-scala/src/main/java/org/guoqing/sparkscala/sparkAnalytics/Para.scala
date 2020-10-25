package org.guoqing.sparkscala.sparkAnalytics

import org.guoqing.sparkscala.config.InitSpark

/**
  * $DESCRIPTION
  *
  * @author DGQ
  * @since 2018/12/18
  */
object Para extends App {
  val sc = InitSpark.getSparkConf()
  val mylist = List("big", "data", "analytics", "hadoop", "spark")
  val myRDD = sc.parallelize(List("big", "data", "analytics", "hadoop", "spark"))
  val t = myRDD.mapPartitionsWithIndex((index, itertor) => itertor)
  println(t)
}
