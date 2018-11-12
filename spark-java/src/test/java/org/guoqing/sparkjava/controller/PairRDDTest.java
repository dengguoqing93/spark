package org.guoqing.sparkjava.controller;

import static org.junit.Assert.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.guoqing.sparkjava.config.InitSpark;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

/**
 * PairRDD Tester.
 *
 * @author DGQ
 * @version 1.0
 * @since <pre>十一月 7, 2018</pre>
 */

public class PairRDDTest {

    @Test
    public void testTranslation() {
        JavaSparkContext sc = InitSpark.sparkContext();
        JavaRDD<Tuple2<Integer, Integer>> input = sc.parallelize(
                Arrays.asList(new Tuple2<>(1, 2), new Tuple2<>(3, 4),
                        new Tuple2<>(3, 6)));
        JavaPairRDD<Integer, Integer> pairInput = input.mapToPair(x -> x);
        System.out.println(
                "合并具有相同键的值：" + pairInput.reduceByKey((x, y) -> x + y).collect());
        System.out.println("对具有相同键的值进行分组:" + pairInput.groupByKey().collect());
        System.out
                .println("对pair中的每个值应用一个函数而不改变键:" + pairInput.mapValues(x -> x +
                        1).collect());
        System.out.println("返回一个仅包含键的RDD：" + pairInput.keys().collect());
        System.out.println("返回一个仅包含值的RDD：" + pairInput.values().collect());
        System.out.println("返回一个根据键排序的RDD：" + pairInput.sortByKey().collect());
    }
} 
