package com.atguigu.spark.core.readwrite

import org.apache.spark.{SparkConf, SparkContext}
import org.mortbay.util.ajax.JSON

object read {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
           val rdd = sc.textFile("o:/hh.txt")
    rdd.map(JSON.parse).foreach(println)



    sc.stop()
  }
}
