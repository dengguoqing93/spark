package org.guoqing.sparkjava.controller;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.guoqing.sparkjava.config.InitSpark;

import java.util.Arrays;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2018/11/12
 */
public class AccumulatorBlanks {
    public static void main(String[] args) {
        JavaSparkContext sc = InitSpark.sparkContext();
        JavaRDD<String> rdd = sc.textFile("file.txt");
        Accumulator<Integer> blankLines = sc.intAccumulator(0);
        JavaRDD<String> callSigns = rdd.flatMap(line -> {
            if (line.equals("")) {
                blankLines.add(1);
            }
            return Arrays.asList(line.split(" ")).iterator();
        });
        callSigns.saveAsTextFile("output-java.txt");
        System.out.println("blank lines:" + blankLines.value());
    }
}
