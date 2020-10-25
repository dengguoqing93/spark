package advanced.analytics

import org.apache.spark.rdd.RDD
import org.guoqing.sparkscala.config.InitSpark

/**
  * $DESCRIPTION
  *
  * @author DGQ
  * @since 2019/3/12
  */
object Chapter2 {
  def main(args: Array[String]): Unit = {
    val sc = InitSpark.getSparkConf()
    val rawBlocks = sc.textFile(
      "file:///Users/dengguoqing/IdeaProjects/spark/spark-scala/src/main/resources/data/block_1.csv")

    /**
      * 获取数据的第一行数据
      */
    printlnFirstRow(rawBlocks)

    println("---------")
    takeAndPrintN(rawBlocks, 10)

    println("---------")
    val head = rawBlocks.take(10)
    head.filter(isHeader).foreach(println)

  }

  /**
    * 打印RDD数据的第一行
    *
    * @param rdd 弹性分布式数据集
    */
  def printlnFirstRow(rdd: RDD[String]): Unit = {
    println(rdd.first)
  }

  /**
    * 获取前num个数据，并打印出来
    *
    * @param rdd 待处理数据集
    * @param num 获取数据个数
    */
  def takeAndPrintN(rdd: RDD[String], num: Int): Unit = {
    if (num > rdd.count()) {
      rdd.foreach(println)
    } else {
      val head = rdd.take(num)
      head.foreach(println)
    }
  }

  /** 判断提供数据是否是文件头
    *
    * @param line 文件行
    * @return
    */
  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }
}
