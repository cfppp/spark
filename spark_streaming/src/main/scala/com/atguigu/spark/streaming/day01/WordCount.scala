package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建streamingcontext
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val context = new StreamingContext(conf,Seconds(5))

    //核心数据集dstreaming
    val socketStream = context.socketTextStream("hadoop102",9999)

    //对dstreaming的操作
    val wordcountDStream = socketStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //最终数据的处理，打印
    wordcountDStream.print(100)

    //启动teamingcontext
    context.start()

    //组织当前线程退出
    context.awaitTermination()


  }
}
