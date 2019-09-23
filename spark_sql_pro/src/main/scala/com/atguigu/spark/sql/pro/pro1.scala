package com.atguigu.spark.sql.pro

import org.apache.spark.sql.SparkSession

object pro1 {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("hh")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/warehouse")
      .getOrCreate()
    session
    import session.implicits._

    session.sql("use sql_project")
    session.sql(
      """
        |select
        |	c.*,
        |	u.click_product_id,
        |	p.product_name
        |from user_visit_action u join city_info c join product_info p
        |on u.city_id = c.city_id and u.click_product_id = p.product_id
        |where u.click_product_id>-1
      """.stripMargin).createOrReplaceTempView("t1")

    session.sql(
      """
        |select
        |	t1.area,
        |	t1.product_name,
        |	count(*) click_count
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    session.sql(
      """
        |select
        |	* ,
        |	row_number() over(partition by area order by click_count desc) rw
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")


    session.sql(
      """
        |select
        |	area,
        |	product_name,
        |	click_count
        |from t3
        |where rw<=3
      """.stripMargin).createOrReplaceTempView("t4")


    session.sql("select * from t4").show()
    //session.sql("show tables").show()





  }
}
