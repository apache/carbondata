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

package org.apache.carbondata.spark.util

import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.DataTypeInfo
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType, DataTypes => CarbonDataTypes, StructField => CarbonStructField}
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat

object CarbonScalaUtil {
  def convertSparkToCarbonDataType(dataType: DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataTypes.STRING
      case ShortType => CarbonDataTypes.SHORT
      case IntegerType => CarbonDataTypes.INT
      case LongType => CarbonDataTypes.LONG
      case DoubleType => CarbonDataTypes.DOUBLE
      case FloatType => CarbonDataTypes.FLOAT
      case DateType => CarbonDataTypes.DATE
      case BooleanType => CarbonDataTypes.BOOLEAN
      case TimestampType => CarbonDataTypes.TIMESTAMP
      case ArrayType(elementType, _) =>
        CarbonDataTypes.createArrayType(CarbonScalaUtil.convertSparkToCarbonDataType(elementType))
      case StructType(fields) =>
        val carbonFields = new util.ArrayList[CarbonStructField]
        fields.map { field =>
          carbonFields.add(
            new CarbonStructField(
              field.name,
              CarbonScalaUtil.convertSparkToCarbonDataType(field.dataType)))
        }
        CarbonDataTypes.createStructType(carbonFields)
      case NullType => CarbonDataTypes.NULL
      case decimal: DecimalType =>
        CarbonDataTypes.createDecimalType(decimal.precision, decimal.scale)
      case _ => throw new UnsupportedOperationException("getting " + dataType + " from spark")
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
    if (CarbonDataTypes.isDecimal(dataType)) {
      DecimalType.SYSTEM_DEFAULT
    } else {
      dataType match {
        case CarbonDataTypes.STRING => StringType
        case CarbonDataTypes.SHORT => ShortType
        case CarbonDataTypes.INT => IntegerType
        case CarbonDataTypes.LONG => LongType
        case CarbonDataTypes.DOUBLE => DoubleType
        case CarbonDataTypes.BOOLEAN => BooleanType
        case CarbonDataTypes.TIMESTAMP => TimestampType
        case CarbonDataTypes.DATE => DateType
      }
    }
  }

  def getString(value: Any,
      serializationNullFormat: String,
      delimiterLevel1: String,
      delimiterLevel2: String,
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat,
      level: Int = 1): String = {
    if (value == null) {
      serializationNullFormat
    } else {
      value match {
        case s: String => if (s.length > CSVInputFormat.MAX_CHARS_PER_COLUMN_DEFAULT) {
          throw new Exception("Dataload failed, String length cannot exceed " +
                              CSVInputFormat.MAX_CHARS_PER_COLUMN_DEFAULT + " characters")
        } else {
          s
        }
        case d: java.math.BigDecimal => d.toPlainString
        case i: java.lang.Integer => i.toString
        case d: java.lang.Double => d.toString
        case t: java.sql.Timestamp => timeStampFormat format t
        case d: java.sql.Date => dateFormat format d
        case b: java.lang.Boolean => b.toString
        case s: java.lang.Short => s.toString
        case f: java.lang.Float => f.toString
        case bs: Array[Byte] => new String(bs,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))
        case s: scala.collection.Seq[Any] =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          s.foreach { x =>
            builder.append(getString(x, serializationNullFormat, delimiterLevel1,
                delimiterLevel2, timeStampFormat, dateFormat, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - 1)
        case m: scala.collection.Map[Any, Any] =>
          throw new Exception("Unsupported data type: Map")
        case r: org.apache.spark.sql.Row =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          for (i <- 0 until r.length) {
            builder.append(getString(r(i), serializationNullFormat, delimiterLevel1,
                delimiterLevel2, timeStampFormat, dateFormat, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - 1)
        case other => other.toString
      }
    }
  }

  def getString(value: String,
      dataType: DataType,
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat): UTF8String = {
    dataType match {
      case TimestampType =>
        UTF8String.fromString(
          DateTimeUtils.timestampToString(timeStampFormat.parse(value).getTime * 1000))
      case DateType =>
        UTF8String.fromString(
          DateTimeUtils.dateToString(
            (dateFormat.parse(value).getTime / DateTimeUtils.MILLIS_PER_DAY).toInt))
      case _ => UTF8String.fromString(value)
    }
  }

  /**
   * This method will validate a column for its data type and check whether the column data type
   * can be modified and update if conditions are met
   *
   * @param dataTypeInfo
   * @param carbonColumn
   */
  def validateColumnDataType(dataTypeInfo: DataTypeInfo, carbonColumn: CarbonColumn): Unit = {
    carbonColumn.getDataType.getName match {
      case "INT" =>
        if (!dataTypeInfo.dataType.equals("bigint") && !dataTypeInfo.dataType.equals("long")) {
          sys
            .error(s"Given column ${ carbonColumn.getColName } with data type ${
              carbonColumn
                .getDataType.getName
            } cannot be modified. Int can only be changed to bigInt or long")
        }
      case "DECIMAL" =>
        if (!dataTypeInfo.dataType.equals("decimal")) {
          sys
            .error(s"Given column ${ carbonColumn.getColName } with data type ${
              carbonColumn.getDataType.getName
            } cannot be modified. Decimal can be only be changed to Decimal of higher precision")
        }
        if (dataTypeInfo.precision <= carbonColumn.getColumnSchema.getPrecision) {
          sys
            .error(s"Given column ${
              carbonColumn
                .getColName
            } cannot be modified. Specified precision value ${
              dataTypeInfo
                .precision
            } should be greater than current precision value ${
              carbonColumn.getColumnSchema
                .getPrecision
            }")
        } else if (dataTypeInfo.scale < carbonColumn.getColumnSchema.getScale) {
          sys
            .error(s"Given column ${
              carbonColumn
                .getColName
            } cannot be modified. Specified scale value ${
              dataTypeInfo
                .scale
            } should be greater or equal to current scale value ${
              carbonColumn.getColumnSchema
                .getScale
            }")
        } else {
          // difference of precision and scale specified by user should not be less than the
          // difference of already existing precision and scale else it will result in data loss
          val carbonColumnPrecisionScaleDiff = carbonColumn.getColumnSchema.getPrecision -
                                               carbonColumn.getColumnSchema.getScale
          val dataInfoPrecisionScaleDiff = dataTypeInfo.precision - dataTypeInfo.scale
          if (dataInfoPrecisionScaleDiff < carbonColumnPrecisionScaleDiff) {
            sys
              .error(s"Given column ${
                carbonColumn
                  .getColName
              } cannot be modified. Specified precision and scale values will lead to data loss")
          }
        }
      case _ =>
        sys
          .error(s"Given column ${ carbonColumn.getColName } with data type ${
            carbonColumn
              .getDataType.getName
          } cannot be modified. Only Int and Decimal data types are allowed for modification")
    }
  }

  /**
   * returns  all fields except tupleId field as it is not required in the value
   *
   * @param fields
   * @return
   */
  def getAllFieldsWithoutTupleIdField(fields: Array[StructField]): Seq[Column] = {
    // getting all fields except tupleId field as it is not required in the value
    val otherFields = fields.toSeq
      .filter(field => !field.name
        .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
      .map(field => {
        new Column(field.name)
      })
    otherFields
  }
}
