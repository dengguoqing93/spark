package org.guoqing.sparkjava.controller;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.hive.HiveContext;
import org.guoqing.sparkjava.config.InitSpark;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * RDD(弹性分布式数据集)操作
 *
 * @author dengguoqing
 * @since 2018-11-06
 */
public class BaseOperationInJava {
    private static JavaSparkContext sc = InitSpark.sparkContext();

    public static void main(String[] args) {

        /*使用parallelize()方法创建RDD*/
        /*JavaRDD<String> linesByPara =
                sc.parallelize(Arrays.asList("pandas", "i like pandas"));

        JavaRDD<String> linesByText = sc.textFile("inputFile.txt");
        BaseOperationInJava operationInJava = new BaseOperationInJava();
        operationInJava.translateOperation()*/
        ;
        /*JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = rdd.map(x -> x * x);
        System.out.println(StringUtils.join(result.collect(), ","));

        PairFunction<String, String, String> keyData =
                x -> new Tuple2<>(x.split(" ")[0], x);
        List input = null;

        String fileName = "";
        JavaPairRDD rdd1 = sc.parallelizePairs(input);
        JavaPairRDD result1 = rdd1.mapToPair(new ConvertToWritableTypes());
        result1.saveAsHadoopFile(fileName, Text.class, IntWritable.class,
                SequenceFileOutputFormat.class);
*/

        HiveContext hiveContext = new HiveContext(sc);
        Dataset<Row> tweets = hiveContext.jsonFile("tweets.json");
        tweets.registerTempTable("tweets");
        Dataset<Row> result = hiveContext.sql("SELECT user.name,text FROM tweets");
        Dataset<String> stringDataset = result.toJSON();
        String[] strings = stringDataset.inputFiles();
        for (String string : strings) {
            System.out.println(string);
        }
        System.out.println("   ");

    }

    public void translateOperation() {
        JavaRDD<String> inputRDD = sc.textFile("log.txt");
        JavaRDD<String> badLinesRDD = inputRDD.filter(
                line -> line.contains("error") || line.contains("warning"));
        System.out.println("Input had " + badLinesRDD.count() + " Concerning lines");
        System.out.println("Here are 10 examples:");
        badLinesRDD.take(10).forEach(line -> System.out.println(line));

    }

    public static class ConvertToWritableTypes
            implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {
        @Override
        public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record)
                throws Exception {
            return new Tuple2<>(new Text(record._1), new IntWritable(record._2));
        }
    }
}
