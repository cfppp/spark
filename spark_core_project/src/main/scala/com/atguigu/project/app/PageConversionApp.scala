package com.atguigu.project.app

import java.text.DecimalFormat

import com.atguigu.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConversionApp {
  def calcPageConversion(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], pages: String): Unit = {
    val splits = pages.split(",")
    val prePage = splits.slice(0, splits.length - 1)
    val postPage = splits.slice(1, splits.length)
    val pageFlow = prePage.zip(postPage).map({
      case (pre, post) => pre + "->" + post
    })
    //计算每个目标跳转流
    val targetPageCount = userVisitActionRDD
      .filter(action => prePage.contains(action.page_id.toString))
      .map(action => (action.page_id, 1)).countByKey()

    //计算每个跳转流的数量
    val totalPageFlows: collection.Map[String, Long] = userVisitActionRDD
      .groupBy(_.session_id)
      .flatMap({
        case (_, actionIt) => {
          val list = actionIt.toList.sortBy(_.action_time)
          val preActions = list.slice(0, list.length - 1)
          val postActions = list.slice(1, list.length)
          val totalPageFlows = preActions.zip(postActions).map(
            {
              case (preAction, postAction) => preAction.page_id + "->" + postAction.page_id
            }
          )
          totalPageFlows.filter(flow => pageFlow.contains(flow)).map((_, 1))
        }
      }).countByKey()

    //最后计算跳转率
    val result = totalPageFlows.map({
      case (flow, flowCount) =>
        val page = flow.split("->")(0)
        val rate = flowCount.toDouble / targetPageCount.getOrElse(page.toLong, Long.MaxValue)
        val formater = new DecimalFormat(".00%")
        (flow, formater.format(rate))
    })


    println(result)
  }
}
