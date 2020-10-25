package advanced.analytics

import org.apache.spark.sql.DataFrame
import org.guoqing.sparkscala.config.InitSpark

/**
  * $DESCRIPTION
  *
  * @author DGQ
  * @since 2019/3/12
  */
object Char2DataFrame {
  val spark = InitSpark.defaultSparkSession()

  def main(args: Array[String]): Unit = {
    val parsed = t02()
    //parsed.groupBy("is_match").count().orderBy($"count".desc).show
    parsed.createOrReplaceTempView("linkage")
    spark.sql("SELECT is_match,COUNT(*) cnt from linkage GROUP BY is_match ORDER BY cnt DESC")
      .show()
  }

  /**
    * 格式化读取的数据，并展示数据结构。
    *
    * @return DataFrame
    */
  def t02(): DataFrame = {
    val parsed = spark.read.option("header", "true").
      option("nullValue", "?").
      option("inferSchema", "true").
      csv("file:///Users/dengguoqing/IdeaProjects/spark/spark-scala/src/main/resources/data")
    parsed.cache()
    parsed
  }

  /**
    * 读取csv文件，并展示
    */
  def t01(): Unit = {
    val prev = spark.read
      .csv("file:///Users/dengguoqing/IdeaProjects/spark/spark-scala/src/main/resources/data")
    prev.show()
  }
}
