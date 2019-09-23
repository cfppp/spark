package com.atguigu.spark.core.transfer

import org.apache.spark.{SparkConf, SparkContext}

object fordByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (9, "huahua"),(2, "lisi"), (3, "wangwu"), (4, "zhaoliu")),2)
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.makeRDD(5 to 14)
    val rdd3 = sc.parallelize(List(("zhangsna",2), ("lisi",6), ("zhangsna",5), ("wangwu",7),("lisi",10)),2)
    val rdd4 = sc.parallelize(List((1, 9), (9, 6),(2, 4), (3, 3), (4, 1)),2)

    /*rdd.sortByKey(false).collect.foreach(println)
    println("************************")
    rdd.mapValues("@" + _).collect.foreach(println)
    println("&&&&&&&&&&&&&&&&&&&&&&")
   rdd.cogroup(rdd4).collect.foreach(println)*/



    sc.stop()
  }
}
