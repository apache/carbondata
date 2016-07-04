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
import org.apache.spark.sql.hive.{CarbonMetaData, DictionaryMap}
import org.apache.spark.sql.types._

import org.carbondata.core.carbon.metadata.datatype.{DataType => CarbonDataType}
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants

object CarbonScalaUtil {
  def convertSparkToCarbonDataType(
      dataType: org.apache.spark.sql.types.DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.STRING
      case ShortType => CarbonDataType.SHORT
      case IntegerType => CarbonDataType.INT
      case LongType => CarbonDataType.LONG
      case DoubleType => CarbonDataType.DOUBLE
      case FloatType => CarbonDataType.FLOAT
      case DateType => CarbonDataType.DATE
      case BooleanType => CarbonDataType.BOOLEAN
      case TimestampType => CarbonDataType.TIMESTAMP
      case ArrayType(_, _) => CarbonDataType.ARRAY
      case StructType(_) => CarbonDataType.STRUCT
      case NullType => CarbonDataType.NULL
      case _ => CarbonDataType.DECIMAL
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

  def convertCarbonToSparkDataType(dataType: CarbonDataType): types.DataType = {
    dataType match {
      case CarbonDataType.STRING => StringType
      case CarbonDataType.SHORT => ShortType
      case CarbonDataType.INT => IntegerType
      case CarbonDataType.LONG => LongType
      case CarbonDataType.DOUBLE => DoubleType
      case CarbonDataType.BOOLEAN => BooleanType
      case CarbonDataType.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case CarbonDataType.TIMESTAMP => TimestampType
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
