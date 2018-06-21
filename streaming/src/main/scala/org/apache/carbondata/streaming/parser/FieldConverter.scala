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

package org.apache.carbondata.streaming.parser

import java.nio.charset.Charset
import java.text.SimpleDateFormat

import org.apache.carbondata.core.constants.CarbonCommonConstants

object FieldConverter {

  /**
   * Return a String representation of the input value
   * @param value input value
   * @param serializationNullFormat string for null value
   * @param delimiterLevel1 level 1 delimiter for complex type
   * @param delimiterLevel2 level 2 delimiter for complex type
   * @param timeStampFormat timestamp format
   * @param dateFormat date format
   * @param isVarcharType whether it is varchar type. A varchar type has no string length limit
   * @param level level for recursive call
   */
  def objectToString(
      value: Any,
      serializationNullFormat: String,
      delimiterLevel1: String,
      delimiterLevel2: String,
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat,
      isVarcharType: Boolean = false,
      level: Int = 1): String = {
    if (value == null) {
      serializationNullFormat
    } else {
      value match {
        case s: String => if (!isVarcharType &&
                              s.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
          throw new Exception("Dataload failed, String length cannot exceed " +
                              CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " characters")
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
            builder.append(objectToString(x, serializationNullFormat, delimiterLevel1,
              delimiterLevel2, timeStampFormat, dateFormat, isVarcharType, level + 1))
              .append(delimiter)
          }
          builder.substring(0, builder.length - delimiter.length())
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
            builder.append(objectToString(r(i), serializationNullFormat, delimiterLevel1,
              delimiterLevel2, timeStampFormat, dateFormat, isVarcharType, level + 1))
              .append(delimiter)
          }
          builder.substring(0, builder.length - delimiter.length())
        case other => other.toString
      }
    }
  }
}
