import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GitHup push counter")
      .master("local").getOrCreate()

    val sc = spark.sparkContext

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/sia/github-archive/2019-11-26-0.json"
    val ghLog = spark.read.json(inputPath)
    val pushes = ghLog.filter("type = 'PushEvent'")

    //1pushes.printSchema()
    //println("all Events: " + ghLog.count())
    //println("only pushes: " + pushes.count())
    //pushes.show(5)

    val grouped = pushes.groupBy("actor.login").count

    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)
  }
}