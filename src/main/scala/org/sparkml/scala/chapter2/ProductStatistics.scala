package org.sparkml.scala.chapter2

import org.apache.spark._


/**
  * Created by jiangnan on 2017/4/16.
  */
object ProductStatistics {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "First Spark App")
    // CSV格式数据转化为(user, product, price)格式的记录集
    val data = sc.textFile("/Users/jiangnan/Work/github/simplespark/src/main/resources/chapter2/products.csv")
      .map(line => line.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    // 购买次数
    val numPurchases = data.count()
    // 多少不同用购买过商品
    val uniqueUsers = data.map{ case (user, product, price) => user }.distinct().count()
    // 求和得出总收入
    val totalRevenue = data.map{ case (user, product, price) => price.toDouble }.sum()
    // 找出最畅销商品
    val productsByPopularity = data.map{ case (user, product, price) => (product, 1) }
      .reduceByKey(_+_)
      .collect()
      .sortBy(-_._2)

    val mostPopular = productsByPopularity(0)
    val mostPopular2 = productsByPopularity.take(2)


    println("Total purchases: " + numPurchases)
    println("Unique users: " + uniqueUsers)
    println("Total revenue: " + totalRevenue)
    println("Most popular prodct: %s , with %d purchases: ".format(mostPopular._1, mostPopular._2))
    println("-----Test-----")
    mostPopular2.foreach(println)

  }
}