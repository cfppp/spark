package com.atguigu.spark.sql.day01.udf

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MyAvg extends UserDefinedAggregateFunction{
  //定义输入参数类型
  override def inputSchema: StructType = StructType(StructField("column",DoubleType):: Nil)
  //定义缓冲区参数类型
  override def bufferSchema: StructType = StructType(StructField("avg",DoubleType)::StructField("count",IntegerType)::Nil)
    //定义最终返回值数据类型
  override def dataType: DataType = DoubleType
  //相同输入是否返回相同输出
  override def deterministic: Boolean = true
  //初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0d
    buffer(1) = 0
  }
  //更新分区内缓冲值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1)=Integer.parseInt(buffer(1).toString) + 1

  }
  //更新分区间换充值
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }
  //最终输出
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)/buffer.getInt(1)
}
