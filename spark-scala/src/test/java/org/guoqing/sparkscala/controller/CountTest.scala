package org.guoqing.sparkscala.controller

/**
  * ${DESCRIPTION}
  *
  * @author dengguoqing
  * @since 2018-11-06
  */
object CountTest {
  def main(args: Array[String]): Unit = {
    val count = new Count
    val inputFile = "spark-scala/inputFile.txt"
    val outputFile = "outputFile"
    count.wordCount(inputFile,outputFile)
  }

}
