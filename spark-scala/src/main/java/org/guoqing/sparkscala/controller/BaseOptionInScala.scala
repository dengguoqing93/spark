package org.guoqing.sparkscala.controller

import org.guoqing.sparkscala.config.InitSpark

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @since 2018-11-06
  */
class BaseOptionInScala {
  val sc = InitSpark.getSparkConf();


  /**
    * RDD转化操作
    */
  def translateOperation(): Unit = {
    val inputRDD = sc.textFile("log.txt")
    val badLinesRDD = inputRDD.filter(line => line.contains("error") || line.contains
    ("warn"))
    println("Input had " + badLinesRDD.count() + " Concerning lines")
    println("Here are 10 examples:")
    badLinesRDD.take(10).foreach(println)
  }
}

object BaseOptionInScalaT extends App {
  val sc = InitSpark.getSparkConf()
  val input = sc.parallelize(List(1, 2, 3, 4))
  val result = input.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1,
                                                                                      acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
  //val result = input.map(x => x * x)
  println(result._1 / result._2.toDouble)
}

object FlatMapTest extends App {
  val sc = InitSpark.getSparkConf()
  val input = sc.parallelize(List("Hello World", "Hi"))
  val result = input.flatMap(line => line.split(" "))
  assert(result.first().equals("Hello"), "测试数据不相等")
}

object Test extends App {
  val sc = InitSpark.getSparkConf()
  val input = sc.parallelize(List(1, 2, 3, 3))
  println("RDD中返回前面的两个元素" + input.top(2).foreach(println))
  println("RDD中元素的个数" + input.count())
  println("RDD各元素出现的次数" + input.countByValue())
  println("RDD中返回两个元素" + input.take(2).foreach(println))
  println("RDD中按照提供的顺序返回最前面的num各元素" + input.takeOrdered(2))
  println("RDD中返回任意一些元素" + input.takeSample(false, 1).foreach(println))
  println("并行整合RDD中所有数据(如sum)" + input.reduce((x, y) => x + y))
  println("提供初始值得的reduce方法" + input.fold(0)((x, y) => x + y))

}