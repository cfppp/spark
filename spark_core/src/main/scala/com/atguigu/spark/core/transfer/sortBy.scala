package com.atguigu.spark.core.transfer

import org.apache.spark.{SparkConf, SparkContext}

object sortBy {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (7, "lisi"), (3, "wangwu"), (4, "zhaoliu"), (6, "hh")))
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.makeRDD(5 to 14)
    val rdd3 = sc.parallelize(List("zhangsna zhangsna", "lisi haha", "zhaoliu zhaoliu", "wangwu wagnwu"))
    rdd.sortBy(aa => aa._1, false).collect.foreach(println)

    sc.stop()
  }
}
