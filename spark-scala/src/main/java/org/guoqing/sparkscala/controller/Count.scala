package org.guoqing.sparkscala.controller

import org.apache.spark.{SparkConf, SparkContext}
import org.guoqing.sparkscala.config.InitSpark

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @since 2018-11-06
  */
class Count {
  def wordCount(inputFile: String, outputFile: String) = {
    /*val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)*/

    val sc = InitSpark.sparkConf()
    val input = sc.textFile(inputFile)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }
    counts.saveAsTextFile(outputFile)
  }
}
