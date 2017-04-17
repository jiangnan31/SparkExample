package org.training.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by jiangnan on 2017/4/16.
 */
public class ProductStatisticsJava {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");

        JavaRDD<String[]> data = sc.textFile("/Users/jiangnan/Work/github/simplespark/src/main/resource/products.csv")
                .map(new Function<String, String[]>() {

                    public String[] call(String s) throws Exception {
                        return s.split(",");
                    }
                });

        // 购买总次数
        long numPurchases = data.count();

        // 有多少个不同客户购买过商品
        long uniqueUsers = data.map(new Function<String[], String>() {
           public String  call(String[] strings) throws Exception {
               return strings[0];
           }
        }).distinct().count();

        // 求和得出总收入一
        double totalRevenue1= data.mapToDouble(new DoubleFunction<String[]>() {
            public double call(String[] strings) throws  Exception {
                return Double.parseDouble(strings[2]);
            }
        }).sum();

        // 求和得出总收入二
        JavaRDD<Double> tempTotalRevenues = data.map(new Function<String[], Double>() {
            public Double call(String[] strings) throws  Exception {
                return Double.parseDouble(strings[2]);
            }
        });
        double totalRevenue2 = tempTotalRevenues.reduce(new Function2<Double, Double, Double>() {
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });

        // 求最畅销的产品是哪个
        // 首先用一个PairFunction和Tuple2类将数据映射成(prodcut, 1)格式的记录
        // 然后,用一个Function2类来调用reduceByKey操作,该操作实际上是一个求和函数
        List<Tuple2<String, Integer>> pairs = data.mapToPair(new PairFunction<String[], String, Integer>() {
            public Tuple2<String, Integer> call(String[] strings) throws Exception {
                return new Tuple2(strings[1], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collect();

        // error
        Collections.sort(pairs, new Comparator<Tuple2<String, Integer>>() {
            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                return -(o1._2() - o2._2());
            }
        });

        String mostPopular = pairs.get(0)._1();
        int purchases = pairs.get(0)._2();


        // 最后对结果进行排序, 注意,这里会需要创建一个Comparator函数来进行降序排列
//        List<Tuple2<String, Integer>> tempPairs = new ArrayList<Tuple2<String, Integer>>(pairs);
//        Collections.sort(tempPairs, new Comparator<Tuple2<String, Integer>>() {
//            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
//                return -(o1._2() - o2._2());
//            }
//        });
//
//        String mostPopular = tempPairs.get(0)._1();
//        int purchases = tempPairs.get(0)._2();
        System.out.println("Total purchases: " + numPurchases);
        System.out.println("Unique users: " + uniqueUsers);
        System.out.println("Total revenue: " + totalRevenue1);
        System.out.println(String.format("Most popular prodct: %s , with %d purchases. ", mostPopular, purchases));
    }
}
