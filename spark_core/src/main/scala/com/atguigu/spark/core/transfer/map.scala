package com.atguigu.spark.core.transfer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object map {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu"), (4, "zhaoliu")))
    val rdd1 = sc.parallelize(Array(1 to 10),3)
    val rdd2 = sc.makeRDD(5 to 14)
    val rdd3 = sc.parallelize(Array("zhangsna zhangsna","lisi haha","zhaoliu zhaoliu","wangwu wagnwu"),3)


    //rdd1.map(aa => (aa, "hh")).collect.foreach(println)
    //rdd.mapPartitionsWithIndex((index,aa ) => aa.map(((_),index) )).collect.foreach(println)
    //rdd3.flatMap( aa => aa.split(" ")).foreach(println)
    //rdd.flatMap(aa => aa._2).collect.foreach(println)
    //rdd1.glom().collect.foreach( aa => aa.foreach(_))

    sc.stop()
  }
}
