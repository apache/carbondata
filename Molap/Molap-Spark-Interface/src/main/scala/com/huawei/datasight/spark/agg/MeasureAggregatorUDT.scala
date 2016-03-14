package com.huawei.datasight.spark.agg

import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator

/**
  * class to support user defined type for molap measure aggregators
  * from spark 1.5, spark has made the data type strict and ANY is no more supported
  * for every data, we need to give the data type
  */
class MeasureAggregatorUDT extends UserDefinedType[MeasureAggregator] {
  //the default DoubleType is Ok as we are not going to pass to spark sql to evaluate,need to add this for compilation errors
  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def serialize(obj: Any): Any = {
    obj match {
      case p: MeasureAggregator => p
    }
  }

  override def deserialize(datum: Any): MeasureAggregator = {
    datum match {
      case values => {
        val xy = values.asInstanceOf[MeasureAggregator]
        xy
      }
    }
  }

  override def userClass: Class[MeasureAggregator] = classOf[MeasureAggregator]

  override def asNullable: MeasureAggregatorUDT = this
}

case object MeasureAggregatorUDT extends MeasureAggregatorUDT