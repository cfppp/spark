package com.atguigu.spark.core.day01

import org.apache.spark.{SparkConf, SparkContext}

object Myscala01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List((1,"zhangsan"),(2,"lisi"),(3,"wangwu"),(4,"zhaoliu")))


    val rdd1 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
    rdd1.foreach(println)
    println("---------------------------")
    val rdd2 = rdd1.mapPartitionsWithIndex((index, aa) => aa.map((index,_)))
    rdd2.foreach(println)



  }
}
