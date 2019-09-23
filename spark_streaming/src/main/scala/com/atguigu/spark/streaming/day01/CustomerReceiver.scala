package com.atguigu.spark.streaming.day01

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.tools.nsc.io.Socket

object CustomerReceiver {
  def main(args: Array[String]): Unit = {

  }
}

class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

  /**
    * 接收启动时候调用的方法
    * 启动一个子线程，循环接收数据
    */
  override def onStart(): Unit = {
    new Thread(){
      override def run()= reveiveData()
    }.start()
  }

  //接收停止时间的回调方法
  override def onStop(): Unit = ???

  //接收数据
  def reveiveData (): Unit = {

    //从socket读数据
    //new Socket("hadoop102",9999)



  }
}