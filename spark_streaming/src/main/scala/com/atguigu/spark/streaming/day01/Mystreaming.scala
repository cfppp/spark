package com.atguigu.spark.streaming.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Mystreaming {

  def main(args: Array[String]): Unit = {
    //获取streamingcontext
    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val stc = new StreamingContext(conf,Seconds(2))

    val dstream = stc
      .socketTextStream("hadoop102",9998)
    val wordcount = dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordcount.print(10)

    //启动socket
    stc.start()

    //防止中途关闭
    stc.awaitTermination()


  }
}
