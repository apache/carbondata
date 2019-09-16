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

package org.apache.spark.sql.util

import java.util.Objects

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.types
import org.apache.spark.sql.types._

import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension, ColumnSchema}

object SparkTypeConverter {

  def createSparkSchema(table: CarbonTable, columns: Seq[String]): StructType = {
    Objects.requireNonNull(table)
    Objects.requireNonNull(columns)
    if (columns.isEmpty) {
      throw new IllegalArgumentException("column list is empty")
    }
    val fields = new java.util.ArrayList[StructField](columns.size)
    val allColumns = table.getTableInfo.getFactTable.getListOfColumns.asScala

    // find the column and add it to fields array
    columns.foreach { column =>
      val col = allColumns.find(_.getColumnName.equalsIgnoreCase(column)).getOrElse(
        throw new IllegalArgumentException(column + " does not exist")
      )
      fields.add(StructField(col.getColumnName, convertCarbonToSparkDataType(col, table)))
    }
    StructType(fields)
  }

  /**
   * Converts from carbon datatype to corresponding spark datatype.
   */
  def convertCarbonToSparkDataType(
      columnSchema: ColumnSchema,
      table: CarbonTable): types.DataType = {
    if (CarbonDataTypes.isDecimal(columnSchema.getDataType)) {
      val scale = columnSchema.getScale
      val precision = columnSchema.getPrecision
      if (scale == 0 && precision == 0) {
        DecimalType(18, 2)
      } else {
        DecimalType(precision, scale)
      }
    } else if (CarbonDataTypes.isArrayType(columnSchema.getDataType)) {
      CarbonMetastoreTypes
        .toDataType(s"array<${ getArrayChildren(table, columnSchema.getColumnName) }>")
    } else if (CarbonDataTypes.isStructType(columnSchema.getDataType)) {
      CarbonMetastoreTypes
        .toDataType(s"struct<${ getStructChildren(table, columnSchema.getColumnName) }>")
    } else if (CarbonDataTypes.isMapType(columnSchema.getDataType)) {
      CarbonMetastoreTypes
        .toDataType(s"map<${ getMapChildren(table, columnSchema.getColumnName) }>")
    } else {
      columnSchema.getDataType match {
        case CarbonDataTypes.STRING => StringType
        case CarbonDataTypes.SHORT => ShortType
        case CarbonDataTypes.INT => IntegerType
        case CarbonDataTypes.LONG => LongType
        case CarbonDataTypes.DOUBLE => DoubleType
        case CarbonDataTypes.FLOAT => FloatType
        case CarbonDataTypes.BYTE => ByteType
        case CarbonDataTypes.BINARY => BinaryType
        case CarbonDataTypes.BOOLEAN => BooleanType
        case CarbonDataTypes.TIMESTAMP => TimestampType
        case CarbonDataTypes.DATE => DateType
        case CarbonDataTypes.VARCHAR => StringType
      }
    }
  }

  def getArrayChildren(table: CarbonTable, dimName: String): String = {
    table.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.getName.toLowerCase match {
        case "array" => s"array<${ getArrayChildren(table, childDim.getColName) }>"
        case "struct" => s"struct<${ getStructChildren(table, childDim.getColName) }>"
        case "map" => s"map<${ getMapChildren(table, childDim.getColName) }>"
        case dType => addDecimalScaleAndPrecision(childDim, dType)
      }
    }).mkString(",")
  }

  def getStructChildren(table: CarbonTable, dimName: String): String = {
    table.getChildren(dimName).asScala.map(childDim => {
      childDim.getDataType.getName.toLowerCase match {
        case "array" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:array<${ getArrayChildren(table, childDim.getColName) }>"
        case "struct" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:struct<${ table.getChildren(childDim.getColName)
          .asScala.map(f => s"${ recursiveMethod(table, childDim.getColName, f) }").mkString(",")
        }>"
        case "map" => s"${
          childDim.getColName.substring(dimName.length + 1)
        }:map<${ getMapChildren(table, childDim.getColName) }>"
        case dType => s"${ childDim.getColName
          .substring(dimName.length() + 1) }:${ addDecimalScaleAndPrecision(childDim, dType) }"
      }
    }).mkString(",")
  }

  def getMapChildren(table: CarbonTable, dimName: String): String = {
    table.getChildren(dimName).asScala.flatMap { childDim =>
      // Map<String, String> is stored internally as Map<Struct<String, String>> in carbon schema
      // and stored as Array<Struct<String, String>> in the actual data storage. So while parsing
      // the map dataType we can ignore the struct child and directly get the children of struct
      // which are actual children of map
      val structChildren = table.getChildren(childDim.getColName).asScala
      structChildren.map { structChild =>
        structChild.getDataType.getName.toLowerCase match {
          case "array" => s"array<${ getArrayChildren(table, structChild.getColName) }>"
          case "struct" => s"struct<${ getStructChildren(table, structChild.getColName) }>"
          case "map" => s"map<${ getMapChildren(table, structChild.getColName) }>"
          case dType => addDecimalScaleAndPrecision(structChild, dType)
        }
      }
    }.mkString(",")
  }

  def addDecimalScaleAndPrecision(dimval: CarbonColumn, dataType: String): String = {
    var dType = dataType
    if (CarbonDataTypes.isDecimal(dimval.getDataType)) {
      dType +=
      "(" + dimval.getColumnSchema.getPrecision + "," + dimval.getColumnSchema.getScale + ")"
    }
    dType
  }

  private def recursiveMethod(
      table: CarbonTable, dimName: String, childDim: CarbonDimension) = {
    childDim.getDataType.getName.toLowerCase match {
      case "array" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:array<${ getArrayChildren(table, childDim.getColName) }>"
      case "struct" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:struct<${ getStructChildren(table, childDim.getColName) }>"
      case "map" => s"${
        childDim.getColName.substring(dimName.length + 1)
      }:map<${ getMapChildren(table, childDim.getColName) }>"
      case dType => s"${
        childDim.getColName
          .substring(dimName.length + 1)
      }:${ addDecimalScaleAndPrecision(childDim, dType) }"
    }
  }

}
