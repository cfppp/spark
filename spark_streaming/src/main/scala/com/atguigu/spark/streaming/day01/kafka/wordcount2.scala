package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object wordcount2 {

  def createSSC(): StreamingContext = {
    val conf = new SparkConf().setAppName("hh").setMaster("local[*]")
    var ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("./ok")
    //kafka的参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "first"
    val group = "bigdata"

    val kafkaParas = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )
    val sourceDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParas,
      Set(topic)
    )

    sourceDStream.print
    ssc
  }

  def main(args: Array[String]): Unit = {


    val ssc = StreamingContext.getActiveOrCreate("./ok", createSSC)

    ssc.start()
    ssc.awaitTermination()


  }


}
