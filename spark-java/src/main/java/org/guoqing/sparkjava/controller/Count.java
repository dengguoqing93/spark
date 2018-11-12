package org.guoqing.sparkjava.controller;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.guoqing.sparkjava.config.InitSpark;
import scala.Tuple2;

import java.util.Arrays;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @since 2018-11-06
 */
public class Count {

    /**
     * 统计输入文件的单词个数，并输出到指定文件
     *
     * @param inputFile  输入文件位置
     * @param outputFile 输出文件位置
     */
    public void wordCount(String inputFile, String outputFile) {
        JavaSparkContext sc = InitSpark.sparkContext();
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> words =
                input.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts =
                words.mapToPair(x -> new Tuple2<>(x, 1))
                        .reduceByKey((x, y) -> (x + y));
        counts.saveAsTextFile(outputFile);
    }
}
