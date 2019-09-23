package com.atguigu.spark.sql.day01.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

class MySum extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("column",DoubleType):: Nil)

  override def bufferSchema: StructType = StructType(StructField("sum", DoubleType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0d

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
     buffer(0) = buffer.getDouble(0) + input.getDouble(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}
