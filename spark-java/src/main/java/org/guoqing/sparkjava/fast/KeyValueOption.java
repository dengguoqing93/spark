package org.guoqing.sparkjava.fast;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.guoqing.sparkjava.config.InitSpark;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 键值对操作
 *
 * @author dengguoqing
 * @date 2019/3/4
 */
public class KeyValueOption {
    private static JavaSparkContext sc = InitSpark.sparkContext();
    private static JavaPairRDD<String, String> pairRDD = sc
            .parallelize(Arrays.asList("I love the girl", "that is ning"))
            .mapToPair(x -> new Tuple2<>(StringUtils.split(x, " ")[0], x));

    public static void main(String[] args) {
        //createPairRDD();
        //filterSecondValue();
        //countWords();

        average();
    }

    /**
     * 创建Pair RDD
     */
    public static void createPairRDD() {
        pairRDD.map(t -> t._1 + t._2).foreach(x -> System.out.println(x));
    }

    /**
     * 根据第二个值过滤数据
     */
    public static void filterSecondValue() {
        JavaPairRDD<String, String> ning = pairRDD
                .filter(t -> t._2.contains("ning"));
        ning.map(t -> t._2).foreach(t -> System.out.println(t));
    }

    public static void countWords() {
        JavaRDD<String> input = sc.parallelize(
                Arrays.asList("I love ning huizhong.",
                        "I don't know when she walks into my life, into my heart . when do i walks into heart ?"));
        JavaRDD<String> words = input
                .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .filter(x -> x.matches("\\w+")).map(String::toUpperCase);
        JavaPairRDD<String, Integer> pairRDD = words
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);
        pairRDD.foreach(t -> System.out.println(t._1 + " " + t._2));

    }

    private static Function<Integer, AvgCount> createAcc = x -> new AvgCount(x,
            1);

    private static Function2<AvgCount, Integer, AvgCount> addAndCount = (a, x) -> new AvgCount(
            a.total + x, a.num + 1);

    private static Function2<AvgCount, AvgCount, AvgCount> combine = (a, b) -> new AvgCount(
            a.total + b.total, a.num + b.num);


    /**
     * 使用combine()求每个键对应的平均值
     */
    public static void average() {
        List<Tuple2<String, Integer>> input = Arrays
                .asList(new Tuple2<>("A", 5), new Tuple2<>("B", 3),
                        new Tuple2<>("G", 3), new Tuple2<>("A", 4),
                        new Tuple2<>("A", 7), new Tuple2<>("B", 9));
        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(input);
        JavaPairRDD<String, Integer> nums = rdd.mapToPair(x -> x);
        JavaPairRDD<String, AvgCount> result = nums
                .combineByKey(createAcc, addAndCount, combine);

        Map<String, AvgCount> countMap = result.collectAsMap();
        countMap.forEach(
                (key, value) -> System.out.println(key + ":" + value.avg()));
    }


    public static class AvgCount implements Serializable {
        public int total;
        public int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public float avg() {
            return total / (float) num;
        }

    }
}
