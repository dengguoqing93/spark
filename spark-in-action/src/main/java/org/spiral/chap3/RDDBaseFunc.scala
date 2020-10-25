package org.spiral.chap3

import org.spiral.common.InitSpark


/**
  * RDD的基本功能测试
  *
  * @author dengguoqing
  * @date 2019/12/30
  * @since 1.0 Version
  * @copyright spiral
  */
class RDDBaseFunc {
  val sc = InitSpark.getSparkConf()

  def count(): Unit = {
    val tranFile = sc.textFile(
      "/Users/dengguoqing/IdeaProjects/spark/spark-in-action/src/main/resources/ch04_data_transactions.txt"
    )

    val tranData = tranFile.map(_.split("#"))

    var transByCust = tranData.map(tran => (tran(2).toInt, tran))

    /*println(transByCust.keys.distinct().count())

    println(transByCust.countByKey())

    val (cid, purch) = transByCust.countByKey().toSeq.maxBy(_._2)

    println(s"cid:$cid , purch:$purch")

    transByCust.lookup(53).foreach(tran => println(tran.mkString(",")))

    /*
     * 使用mapValues变换更改键值对RDD中的值
     */

    transByCust = transByCust.mapValues(tran => {
      if (tran(3).toInt == 25 && tran(4).toDouble > 1)
        tran(5) = (tran(5).toDouble * 0.95).toString
      tran
    })

    /*
      使用flatMapValues转换将值添加到键
     */
    transByCust = transByCust.flatMapValues(tran => {
      if (tran(3).toInt == 81 && tran(4).toDouble >= 5) {
        val cloned = tran.clone()
        cloned(5) = "0.00";
        cloned(3) = "70";
        cloned(4) = "1"
        List(tran, cloned)
      } else
        List(tran)
    })

    println("flatMapValues处理后的数据")
    transByCust.foreach(tran => println(tran._2.mkString(",")))*/

    val prods = transByCust.aggregateByKey(List[String]())(
      (prods, tran) => prods ::: List(tran(3)),
      (prods1, prods2) => prods1 ::: prods2
    )
    prods.collect().foreach(println)
  }

}

object RDDBaseFunc {

  def main(args: Array[String]): Unit = {
    val rdd = new RDDBaseFunc
    rdd.count()
  }

}
