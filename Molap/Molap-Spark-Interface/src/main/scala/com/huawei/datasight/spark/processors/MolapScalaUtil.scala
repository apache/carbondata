package com.huawei.datasight.spark.processors

import org.apache.spark.sql.types._
import com.huawei.unibi.molap.engine.expression.{DataType => MolapDataType}
import com.huawei.unibi.molap.constants.MolapCommonConstants
import org.apache.spark.sql.cubemodel.Level

object MolapScalaUtil {
  def convertSparkToMolapDataType(dataType: org.apache.spark.sql.types.DataType): MolapDataType =
    dataType match {

      case StringType => MolapDataType.StringType
      case IntegerType => MolapDataType.IntegerType
      case LongType => MolapDataType.LongType
      case DoubleType => MolapDataType.DoubleType
      case FloatType => MolapDataType.FloatType
      case DateType => MolapDataType.DateType
      case BooleanType => MolapDataType.BooleanType
      case TimestampType => MolapDataType.TimestampType
      case ArrayType(_,_) => MolapDataType.ArrayType
      case StructType(_) => MolapDataType.StructType
      case NullType => MolapDataType.NullType
    }

  def convertSparkToMolapSchemaDataType(dataType: String): String =

    dataType match {
      case MolapCommonConstants.STRING_TYPE => MolapCommonConstants.STRING
      case MolapCommonConstants.INTEGER_TYPE => MolapCommonConstants.INTEGER
      case MolapCommonConstants.BYTE_TYPE => MolapCommonConstants.INTEGER
      case MolapCommonConstants.SHORT_TYPE => MolapCommonConstants.INTEGER
      case MolapCommonConstants.LONG_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.DOUBLE_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.FLOAT_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.DECIMAL_TYPE => MolapCommonConstants.NUMERIC
      case MolapCommonConstants.DATE_TYPE => MolapCommonConstants.STRING
      case MolapCommonConstants.BOOLEAN_TYPE => MolapCommonConstants.STRING
      case MolapCommonConstants.TIMESTAMP_TYPE => MolapCommonConstants.TIMESTAMP
      case anyType => anyType
    }
  
  def convertSparkColumnToMolapLevel(field : (String,String)): Seq[Level] = 
    field._2 match {
      case MolapCommonConstants.STRING_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.STRING))
      case MolapCommonConstants.INTEGER_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.INTEGER))
      case MolapCommonConstants.BYTE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.INTEGER))
      case MolapCommonConstants.SHORT_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.INTEGER))
      case MolapCommonConstants.LONG_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.DOUBLE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.FLOAT_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.DECIMAL_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.NUMERIC))
      case MolapCommonConstants.DATE_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.STRING))
      case MolapCommonConstants.BOOLEAN_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.STRING))
      case MolapCommonConstants.TIMESTAMP_TYPE => Seq(Level(field._1, field._1, Int.MaxValue, MolapCommonConstants.TIMESTAMP))
  	}
}