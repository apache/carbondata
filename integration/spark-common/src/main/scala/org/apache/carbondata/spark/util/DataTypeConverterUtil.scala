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

import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.format.{DataType => ThriftDataType}

object DataTypeConverterUtil {
  val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_DECIMALTYPE = """decimaltype\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  def convertToCarbonType(dataType: String): DataType = {
    dataType.toLowerCase match {
      case "string" => DataType.STRING
      case "int" => DataType.INT
      case "integer" => DataType.INT
      case "tinyint" => DataType.SHORT
      case "smallint" => DataType.SHORT
      case "long" => DataType.LONG
      case "bigint" => DataType.LONG
      case "numeric" => DataType.DOUBLE
      case "double" => DataType.DOUBLE
      case "float" => DataType.DOUBLE
      case "decimal" => DataType.DECIMAL
      case FIXED_DECIMAL(_, _) => DataType.DECIMAL
      case "timestamp" => DataType.TIMESTAMP
      case "date" => DataType.DATE
      case "array" => DataType.ARRAY
      case "struct" => DataType.STRUCT
      case _ => convertToCarbonTypeForSpark2(dataType)
    }
  }

  def convertToCarbonTypeForSpark2(dataType: String): DataType = {
    dataType.toLowerCase match {
      case "stringtype" => DataType.STRING
      case "inttype" => DataType.INT
      case "integertype" => DataType.INT
      case "tinyinttype" => DataType.SHORT
      case "shorttype" => DataType.SHORT
      case "longtype" => DataType.LONG
      case "biginttype" => DataType.LONG
      case "numerictype" => DataType.DOUBLE
      case "doubletype" => DataType.DOUBLE
      case "floattype" => DataType.DOUBLE
      case "decimaltype" => DataType.DECIMAL
      case FIXED_DECIMALTYPE(_, _) => DataType.DECIMAL
      case "timestamptype" => DataType.TIMESTAMP
      case "datetype" => DataType.DATE
      case others =>
        if (others != null && others.startsWith("arraytype")) {
          DataType.ARRAY
        } else if (others != null && others.startsWith("structtype")) {
          DataType.STRUCT
        } else if (others != null && others.startsWith("char")) {
          DataType.STRING
        } else if (others != null && others.startsWith("varchar")) {
          DataType.STRING
        } else {
          sys.error(s"Unsupported data type: $dataType")
        }
    }
  }

  def convertToString(dataType: DataType): String = {
    dataType match {
      case DataType.STRING => "string"
      case DataType.SHORT => "smallint"
      case DataType.INT => "int"
      case DataType.LONG => "bigint"
      case DataType.DOUBLE => "double"
      case DataType.FLOAT => "double"
      case DataType.DECIMAL => "decimal"
      case DataType.TIMESTAMP => "timestamp"
      case DataType.DATE => "date"
      case DataType.ARRAY => "array"
      case DataType.STRUCT => "struct"
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
      case "short" => ThriftDataType.SHORT
      case "long" | "bigint" => ThriftDataType.LONG
      case "double" => ThriftDataType.DOUBLE
      case "decimal" => ThriftDataType.DECIMAL
      case "date" => ThriftDataType.DATE
      case "timestamp" => ThriftDataType.TIMESTAMP
      case "array" => ThriftDataType.ARRAY
      case "struct" => ThriftDataType.STRUCT
      case _ => ThriftDataType.STRING
    }
  }
}
