package org.spiral.chap06

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.streaming._
import org.spiral.common.InitSpark

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @date 2020/1/4
  * @since 1.0 Version
  * @copyright spiral
  */
object SparkStreamAction {
  val sc = InitSpark.getSparkConf()

  val spark = InitSpark.defaultSparkSession()

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc, Seconds(5))

    val fileStream = ssc
      .textFileStream("/Users/dengguoqing/Downloads/first-edition-master/ch06/orders.txt")

    val orders = fileStream.flatMap(line => {
      val datePattern = DateTimeFormatter.ofPattern("YYYYMMdd HH:mm:ss")
      val s = line.split(",")
      try {
        assert(s(6) == "B" || s(6) == "S")
        println(line)
        List(
          Order(LocalDateTime.parse(s(0), datePattern), s(1).toLong, s(2).toLong, s(3), s(4).toInt,
            s(5).toDouble, s(6) == "B"))
      } catch {
        case e: Throwable => println("wrong line format(" + e + "):" + line)
          List()
      }
    })

    val rf = new RandomForestClassifier()

    rf.setMaxDepth(20)


  }

  case class Order(times: LocalDateTime, orderId: Long, clientId: Long, symbol: String, amount: Int,
                   price: Double, buy: Boolean)

}
