package org.sparkml.java.chapter2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jiangnan on 2017/4/16.
 */
public class WordCountJava {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");

        JavaRDD<String> data = sc.textFile("/Users/jiangnan/Work/utils/spark-hadoop/README.md")
                .flatMap(new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        List<String> temp = Arrays.asList(s.split(" "));
                        return temp.iterator();
                    }
                }).filter(new Function<String, Boolean>() {
                    public Boolean call(String v1) throws Exception {
                        return v1.length() > 1;
                    }
                });

        data.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return  new Tuple2(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).saveAsTextFile("/Users/jiangnan/Downloads/tempFile/output_java");


    }
}
