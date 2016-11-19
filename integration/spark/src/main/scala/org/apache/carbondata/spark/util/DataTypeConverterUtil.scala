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

import org.apache.carbondata.core.carbon.metadata.datatype.DataType

object DataTypeConverterUtil {
  def convertToCarbonType(dataType: String): DataType = {
    dataType.toLowerCase match {
      case "string" => DataType.STRING
      case "int" => DataType.INT
      case "integer" => DataType.INT
      case "tinyint" => DataType.SHORT
      case "short" => DataType.SHORT
      case "long" => DataType.LONG
      case "bigint" => DataType.LONG
      case "numeric" => DataType.DOUBLE
      case "double" => DataType.DOUBLE
      case "decimal" => DataType.DECIMAL
      case "timestamp" => DataType.TIMESTAMP
      case "array" => DataType.ARRAY
      case "struct" => DataType.STRUCT
      case _ => sys.error(s"Unsupported data type: $dataType")
    }
  }

  def convertToString(dataType: DataType): String = {
    dataType match {
      case DataType.STRING => "string"
      case DataType.SHORT => "smallint"
      case DataType.INT => "int"
      case DataType.LONG => "bigint"
      case DataType.DOUBLE => "double"
      case DataType.DECIMAL => "decimal"
      case DataType.TIMESTAMP => "timestamp"
      case DataType.ARRAY => "array"
      case DataType.STRUCT => "struct"
    }
  }
}
