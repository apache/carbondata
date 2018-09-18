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

import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.format.{DataType => ThriftDataType}

object DataTypeConverterUtil {
  val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_DECIMALTYPE = """decimaltype\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  def convertToCarbonType(dataType: String): DataType = {
    dataType.toLowerCase match {
      case "boolean" => DataTypes.BOOLEAN
      case "string" => DataTypes.STRING
      case "int" => DataTypes.INT
      case "integer" => DataTypes.INT
      case "tinyint" => DataTypes.SHORT
      case "smallint" => DataTypes.SHORT
      case "short" => DataTypes.SHORT
      case "long" => DataTypes.LONG
      case "bigint" => DataTypes.LONG
      case "numeric" => DataTypes.DOUBLE
      case "double" => DataTypes.DOUBLE
      case "float" => DataTypes.DOUBLE
      case "decimal" => DataTypes.createDefaultDecimalType
      case FIXED_DECIMAL(_, _) => DataTypes.createDefaultDecimalType
      case "timestamp" => DataTypes.TIMESTAMP
      case "date" => DataTypes.DATE
      case "binary" => DataTypes.BINARY
      case "array" => DataTypes.createDefaultArrayType
      case "struct" => DataTypes.createDefaultStructType
      case "map" => DataTypes.createDefaultMapType
      case _ => convertToCarbonTypeForSpark2(dataType)
    }
  }

  def convertToCarbonTypeForSpark2(dataType: String): DataType = {
    dataType.toLowerCase match {
      case "booleantype" => DataTypes.BOOLEAN
      case "stringtype" => DataTypes.STRING
      case "inttype" => DataTypes.INT
      case "integertype" => DataTypes.INT
      case "tinyinttype" => DataTypes.SHORT
      case "shorttype" => DataTypes.SHORT
      case "longtype" => DataTypes.LONG
      case "biginttype" => DataTypes.LONG
      case "numerictype" => DataTypes.DOUBLE
      case "doubletype" => DataTypes.DOUBLE
      case "floattype" => DataTypes.DOUBLE
      case "decimaltype" => DataTypes.createDefaultDecimalType
      case FIXED_DECIMALTYPE(_, _) => DataTypes.createDefaultDecimalType
      case "timestamptype" => DataTypes.TIMESTAMP
      case "datetype" => DataTypes.DATE
      case "binarytype" => DataTypes.BINARY
      case others =>
        if (others != null && others.startsWith("arraytype")) {
          DataTypes.createDefaultArrayType()
        } else if (others != null && others.startsWith("structtype")) {
          DataTypes.createDefaultStructType()
        } else if (others != null && others.startsWith("maptype")) {
          DataTypes.createDefaultMapType
        } else if (others != null && others.startsWith("char")) {
          DataTypes.STRING
        } else if (others != null && others.startsWith("varchar")) {
          DataTypes.STRING
        } else {
          CarbonException.analysisException(s"Unsupported data type: $dataType")
        }
    }
  }

  /**
   * convert from wrapper to external data type
   *
   * @param dataType
   * @return
   */
  def convertToThriftDataType(dataType: String): ThriftDataType = {
    if (null == dataType) {
      return null
    }
    dataType match {
      case "string" => ThriftDataType.STRING
      case "int" => ThriftDataType.INT
      case "boolean" => ThriftDataType.BOOLEAN
      case "short" => ThriftDataType.SHORT
      case "long" | "bigint" => ThriftDataType.LONG
      case "double" => ThriftDataType.DOUBLE
      case "decimal" => ThriftDataType.DECIMAL
      case "date" => ThriftDataType.DATE
      case "timestamp" => ThriftDataType.TIMESTAMP
      case "binary" => ThriftDataType.BINARY
      case "array" => ThriftDataType.ARRAY
      case "struct" => ThriftDataType.STRUCT
      case "map" => ThriftDataType.MAP
      case "varchar" => ThriftDataType.VARCHAR
      case _ => ThriftDataType.STRING
    }
  }
}
