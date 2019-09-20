package com.atguigu.spark.core.transfer

import org.apache.spark.{SparkConf, SparkContext}

object sample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu"), (4, "zhaoliu")))
    val rdd1 = sc.parallelize(1 to 10)
    val rdd2 = sc.makeRDD(5 to 14)
    val rdd3 = sc.parallelize(List("zhangsna zhangsna", "lisi haha", "zhaoliu zhaoliu", "wangwu wagnwu"))

    //rdd1.sample(false,0.4).foreach(println)
    rdd1.sample(false,0.3,System.currentTimeMillis()).foreach(println)

    sc.stop()
  }
}
