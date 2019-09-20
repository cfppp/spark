package com.atguigu.spark.core.project

import org.apache.spark.{SparkConf, SparkContext}

object projSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("M:/intelliJ/git/spark/spark_core/src/main/resources/agent.log")
    //将文件中数据根据每行进行split并获取不表字段
    val rdd = lines.map(line => {
      val split = line.split(" ")
      ((split(1), split(4)), 1)
    })
    //根据城市、订单聚合数据
    val rdd1 = rdd.reduceByKey(_ + _)

    //将聚合后的数据map，获取想要的数据类型
    val rdd2 = rdd1.map(aa => (aa._1._1.toInt, (aa._1._2.toInt, aa._2)))
    //根据城市、订单的顺序进行排序
    //val rdd3 = rdd2.toList.groupBy()
    val rdd3 = rdd2.groupByKey()
    //将数据集的值进行变换：
    val rdd4 = rdd3.mapValues(aa => aa.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    //进行最终的排序
    rdd4.sortBy(_._1).collect.foreach(println)


    sc.stop()
  }
}

/**
  * 分析：
  *
  */

