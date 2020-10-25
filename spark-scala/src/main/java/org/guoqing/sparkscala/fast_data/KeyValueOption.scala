package org.guoqing.sparkscala

import org.guoqing.sparkscala.config.InitSpark

/**
  * $DESCRIPTION
  *
  * @author DGQ
  * @since 2019/3/4
  *
  */

case class Store(string: String)

object KeyValueOption {

  val sc = InitSpark.getSparkConf()

  def main(args: Array[String]): Unit = {
    /* 键值对的简单操作
    val lines = sc.parallelize(List("you are ", "nice"))
    val pairs = lines.map(x => (x.split(" ")(0), x))
    pairs.map { case (x, y) => x }.foreach(println(_))
    */
    //值对映射
    /*val pairs = sc
      .parallelize(List(("panda", 0),
        ("pink", 3), ("pirate", 3),
        ("panda", 1), ("pink", 4)))
    pairs.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .foreach(println(_))*/
    //wordCounts()
    //average()
    testJoin()
  }

  /**
    * 单词计数
    */
  def wordCounts(): Unit = {
    val lines = sc.parallelize(List
    ("I love ning huizhong.",
      "I don't know when she walks into my life, into my heart . when do i walks into heart ?"))

    val words = lines.flatMap(t => t.split(" "))
    val result = words.map(t => (t, 1)).reduceByKey((x, y) => x + y)
    result.foreach(println(_))
  }

  /**
    * 使用combineByKey()求每个键对应的平均值
    */
  def average(): Unit = {
    val input = List(("A", 5), ("B", 3), ("G", 3), ("A", 4), ("A", 7), ("B", 9))
    val rdd = sc.parallelize(input)

    val result = rdd.combineByKey(v => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).
      map { case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().map(println(_))
  }

  def testJoin(): Unit = {
    val storeAddress = sc.parallelize(Seq((Store("Ritual"), "1026 Valencia St"),
      (Store("  "), "748 Van Ness Ave"),
      (Store("Philz"), "3101 24th St"),
      (Store("Starbucks"), "Seattle")))
    val storeRating = sc.parallelize(Seq((Store("Ritual"), 4.9), (Store("Philz"), 4.8)))
    val result = storeAddress.join(storeRating)
    result.foreach(t => println(t._1 + ":" + t._2))

  }
}
