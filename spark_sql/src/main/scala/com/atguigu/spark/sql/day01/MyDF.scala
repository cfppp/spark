package com.atguigu.spark.sql.day01

import com.atguigu.spark.sql.day01.udf.{MyAvg, MySum}
import org.apache.spark.sql.SparkSession

object MyDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hh").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val rdd = sc.parallelize(List(User("zhangsan", 12, "nan"),User("wangwu", 56, "nan"), User("lisi", 20, "nv"), User("zhaoliu", 70, "nv")))



    /*val df = rdd.toDF()
    df.createOrReplaceTempView("user")
    spark.sql("select name,age from user").show()*/

   /* spark.udf.register("sum" ,new MySum)
    spark.udf.register("avg",new MyAvg)

    spark.sql("select sum(age) sumage from user").show
    spark.sql("select avg(age) avg from user").show*/

    val df = rdd.toDF()
    val ds = df.as[User]
    val rr = df.rdd
    val rdd1 = ds.rdd
    val ddf = ds.toDF()




    spark.close()
  }
}


case class User( name: String,  age: Int, sex: String)