/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
      case _ => MolapDataType.DecimalType
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