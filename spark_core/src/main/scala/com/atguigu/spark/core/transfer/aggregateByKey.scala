package com.atguigu.spark.core.transfer

import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu"), (4, "zhaoliu")))
    val rdd1 = sc.parallelize(List(("monkey", 4), ("lili", 3), ("map", 5), ("lili", 2), ("mali", 7), ("map", 5), ("mali", 2)), 3)
    val rdd2 = sc.parallelize(List("zhangsna zhangsna", "lisi haha", "zhaoliu zhaoliu", "wangwu wagnwu"))
    val rdd4 = rdd1.aggregateByKey(1)(math.min(_, _), _ + _)


    val rdd5 = rdd1.mapPartitionsWithIndex((index, aa) => aa.map((index, _)))

    rdd1.foldByKey(0)(_+_).foreach(println)
    println("$$$$$$$$$$$$")

    sc.stop()
  }
}
