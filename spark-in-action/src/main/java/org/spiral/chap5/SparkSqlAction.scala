package org.spiral.chap5

import org.apache.spark.sql.types._
import org.spiral.common.InitSpark


/**
  * sparkSql应用实战
  *
  * @author dengguoqing
  * @date 2020/1/2
  * @since 1.0 Version
  * @copyright spiral
  */
class SparkSqlAction {}

object SparkSqlAction {

  val sc = InitSpark.getSparkConf()

  val spark = InitSpark.defaultSparkSession()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.expressions.Window
    val itPostsRows = sc.textFile(
      "/Users/dengguoqing/IdeaProjects/spark/spark-in-action/src/main/resources/italianPosts.csv")

    val itPostsSplit = itPostsRows.map(x => x.split("~"))

    val itPostsRDD = itPostsSplit.map(x => (
      x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12)
    ))

    val postSchema = StructType(Seq(StructField("commentCount", IntegerType, true),
      StructField("lastActivityDate", TimestampType, true),
      StructField("ownerUserId", LongType, true), StructField("body", StringType, true),
      StructField("score", IntegerType, true), StructField("creationDate", TimestampType, true),
      StructField("viewCount", IntegerType, true), StructField("title", StringType, true),
      StructField("tags", StringType, true), StructField("answerCount", IntegerType, true),
      StructField("acceptedAnswerId", LongType, true), StructField("postTypeId", LongType, true),
      StructField("id", LongType, false)))


    val itPostsDFrame = itPostsRDD.toDF()

    //itPostsDFrame.show(10)

    //itPostsDFrame.printSchema()
    val itPostsDFrameComment = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
    //itPostsDFrameComment.printSchema()
    val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()

    //itPostsDFCase.printSchema()
    val postsDF = itPostsDFCase

    /*val postIdBody = postsDF.select("id", "body")

    val count = postIdBody.filter('body contains "Italiano").count()

    println(count)*/
    import org.apache.spark.sql.functions._
    postsDF.filter('postTypeId === 1).select('ownerUserId, 'acceptedAnswerId, 'score,
      max('score).over(Window.partitionBy('ownerUserId)) as "maxPerUser")
      .withColumn("toMax", 'maxPerUser - 'score).sortWithinPartitions($"score").show(100)

    postsDF.filter($"postTypeId" === 1).select($"ownerUserId", 'id, 'creationDate,
      lag('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "prev",
      lead('id, 1).over(Window.partitionBy('ownerUserId).orderBy('creationDate)) as "next").
      orderBy('ownerUserId, 'id).show(10)

  }

  import StringImplicits._

  def stringToPost(row: String): Post = {
    val r = row.split("~")
    Post(r(0).toIntSafe, r(1).toTimestampSafe, r(2).toLongSafe, r(3), r(4).toIntSafe,
      r(5).toTimestampSafe, r(6).toIntSafe, r(7), r(8), r(9).toIntSafe, r(10).toLongSafe,
      r(11).toLongSafe, r(12).toLong)
  }
}
