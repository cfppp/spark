package com.atguigu.spark.streaming.day01.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MyKafka {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hh").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))

    //Kafka的参数声明
    val brokers = "hadooop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "first"
    val group = "bigdata"

    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )


    //泛型1、2是key、value的类型  泛型3、4是12的解码器StringDecoder
    val sourceDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set(topic))
    sourceDStream.print


    ssc.start()
    ssc.awaitTermination()

  }
}
