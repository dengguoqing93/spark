package org.guoqing.sparkjava.controller;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.guoqing.sparkjava.config.InitSpark;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Count Tester.
 *
 * @author DGQ
 * @version 1.0
 * @since <pre>十一月 6, 2018</pre>
 */

public class CountTest {
    private static JavaSparkContext sc = InitSpark.sparkContext();

    /**
     * Method: wordCount(String inputFile, String outputFile)
     */
    @Test
    public void testWordCount() throws Exception {
        String inputFile = "inputFile.txt";
        String outputFile = "outputFile";
        Count count = new Count();
        count.wordCount(inputFile, outputFile);
    }

    @Test
    public void testFlatMap() {
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("Hello World", "hi"));
        JavaRDD<String> words =
                lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        assertEquals(words.first(), "Hello");
    }

} 
