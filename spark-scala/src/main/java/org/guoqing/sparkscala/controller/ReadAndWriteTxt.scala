package org.guoqing.sparkscala.controller

import org.guoqing.sparkscala.config.InitSpark

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @since 2018-11-09
  */
class ReadAndWriteTxt {
  val sc = InitSpark.getSparkConf()

  def calMean() = {
    val input = sc.wholeTextFiles("file:///Users/dengguoqing/sparkData")
    val result = input.mapValues { y =>
      val nums = y.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }
    result
  }
}

object ReadAndWriteTxt extends App{
  val readAndWriteTxt = new ReadAndWriteTxt
  val result = readAndWriteTxt.calMean()
  result.saveAsTextFile("output")
}
