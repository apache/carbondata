/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.carbondata.spark.util

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.Level
import org.apache.spark.sql.hive.{CarbonMetaData, DictionaryMap}
import org.apache.spark.sql.types._

import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.scan.expression.{DataType => CarbonDataType}

object CarbonScalaUtil {
  def convertSparkToCarbonDataType(
      dataType: org.apache.spark.sql.types.DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.StringType
      case ShortType => CarbonDataType.ShortType
      case IntegerType => CarbonDataType.IntegerType
      case LongType => CarbonDataType.LongType
      case DoubleType => CarbonDataType.DoubleType
      case FloatType => CarbonDataType.FloatType
      case DateType => CarbonDataType.DateType
      case BooleanType => CarbonDataType.BooleanType
      case TimestampType => CarbonDataType.TimestampType
      case ArrayType(_, _) => CarbonDataType.ArrayType
      case StructType(_) => CarbonDataType.StructType
      case NullType => CarbonDataType.NullType
      case _ => CarbonDataType.DecimalType
    }
  }

  def convertSparkToCarbonSchemaDataType(dataType: String): String = {
    dataType match {
      case CarbonCommonConstants.STRING_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.INTEGER_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.BYTE_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.SHORT_TYPE => CarbonCommonConstants.SHORT
      case CarbonCommonConstants.LONG_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DOUBLE_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.FLOAT_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DECIMAL_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DATE_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.BOOLEAN_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.TIMESTAMP_TYPE => CarbonCommonConstants.TIMESTAMP
      case anyType => anyType
    }
  }

  def convertCarbonToSparkDataType(dataType: DataType): types.DataType = {
    dataType match {
      case DataType.STRING => StringType
      case DataType.SHORT => ShortType
      case DataType.INT => IntegerType
      case DataType.LONG => LongType
      case DataType.DOUBLE => DoubleType
      case DataType.BOOLEAN => BooleanType
      case DataType.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case DataType.TIMESTAMP => TimestampType
    }
  }

  object CarbonSparkUtil {

    def createSparkMeta(carbonTable: CarbonTable): CarbonMetaData = {
      val dimensionsAttr = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
                           .asScala.map(x => x.getColName) // wf : may be problem
      val measureAttr = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
                        .asScala.map(x => x.getColName)
      val dictionary =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName).asScala.map { f =>
        (f.getColName.toLowerCase,
          f.hasEncoding(Encoding.DICTIONARY) && !f.hasEncoding(Encoding.DIRECT_DICTIONARY))
      }
      CarbonMetaData(dimensionsAttr, measureAttr, carbonTable, DictionaryMap(dictionary.toMap))
    }
  }

}
