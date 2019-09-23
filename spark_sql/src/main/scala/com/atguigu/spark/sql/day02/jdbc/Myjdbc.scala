package com.atguigu.spark.sql.day02.jdbc

import org.apache.spark.sql.SparkSession


object Myjdbc {
  def main(args: Array[String]): Unit = {


    val session = SparkSession.builder().appName("hh").master("local[2]").getOrCreate()
    import session.implicits._
    //val rdd = session.sparkContext.parallelize(List(User("zhangsan", 12, "nan"),User("wangwu", 56, "nan"), User("lisi", 20, "nv"), User("zhaoliu", 70, "nv")))
    //val df = rdd.toDF()

    val jdbcDF = session.read.format("jdbc")
      //.option("driver","Class.forName(com.mysql.jdbc.Driver)")
      .option("url", "jdbc:mysql://hadoop102:3306/savelife")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "visit")
      .load()
    jdbcDF.show()
      //df.createOrReplaceTempView("visit")
    //session.sql("select * from savelife.visit").show()


    session.close()
  }
}

case class User(name: String, age: Int, sex : String)