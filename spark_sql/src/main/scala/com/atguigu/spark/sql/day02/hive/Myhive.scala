package com.atguigu.spark.sql.day02.hive

import org.apache.spark.sql.SparkSession

object Myhive {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("hh")
      .master("local[2]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()
    session.sql("show databases").show()


  }

}
