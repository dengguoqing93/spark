package datasource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
  * 列格式文件处理
  *
  * @author DGQ
  * @since 2019/2/27
  */
object Parquet extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example").getOrCreate()
    val schema = new StructType().add("name", StringType, true).add("age", IntegerType, true)
    val df = spark.read.schema(schema)
      .json("/Users/dengguoqing/IdeaProjects/spark/spark-sql/src/main/resources/people.json")

    df.write.parquet("people.parquet")

    val parquetFileDF = spark.read.parquet("people.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 45")
    import spark.implicits._
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    namesDF.as[String]
  }

}
