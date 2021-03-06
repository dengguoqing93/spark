package org.guoqing.sparkjava.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark配置
 *
 * @author dengguoqing
 * @since 2018-11-06
 */
public class InitSpark {
    public static JavaSparkContext sparkContext(String... args) {
        String master;
        String appName;
        if (null == args || args.length == 0) {
            master = "local";
            appName = "My App";
        } else if (args.length == 2) {
            master = args[0];
            appName = args[1];
        } else {
            throw new IllegalArgumentException("参数长度错误");
        }
        SparkConf conf = new SparkConf().setMaster(master).setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;
    }


    public static SparkSession sparkSessionBuild() {
        return SparkSession.builder().appName("test")
                           .config("configuration_key", "configuration_value")
                           .enableHiveSupport().getOrCreate();
    }

    public static void main(String[] args) {
        SparkSession sparkSession = sparkSessionBuild();
        Dataset<Row> json = sparkSession.read().json("people.json");

    }
}
