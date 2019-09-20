package com.atguigu.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object countByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu"), (4, "zhaoliu")))
    val rdd1 = sc.parallelize(List(("monkey", 4), ("lili", 3), ("map", 5), ("lili", 2)))
    val rdd2 = sc.parallelize(List("zhangsna zhangsna", "lisi haha", "zhaoliu zhaoliu", "wangwu wagnwu"))
    rdd1.countByKey().foreach(println)
    println("@@@@@@@@@@@@@@@@@@")
    val rdd3 = rdd.countByValue()


    sc.stop()
  }
}
