package com.atguigu.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (2, "lisi"), (7, "make"),(3, "wangwu"), (4, "zhaoliu")))
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.makeRDD(5 to 14)
    val rdd3 = sc.parallelize(List("zhangsna zhangsna", "lisi haha", "zhaoliu zhaoliu", "wangwu wagnwu"))

    //println(rdd1.reduce(_ * _))
    //println(rdd3.reduce(_ + "," + _))
    //println(rdd2.reduce(_ - _))//??


    //rdd3.take(7).foreach(println)
    val rdd4 = rdd.sortByKey(false)
    rdd4.takeOrdered(3).foreach(println)
    rdd3.saveAsTextFile("d:/hh.txt")
    sc.stop()
  }
}
