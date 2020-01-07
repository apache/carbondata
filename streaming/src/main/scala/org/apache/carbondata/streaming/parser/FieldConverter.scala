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
import java.util
import java.util.Base64

import org.apache.carbondata.core.constants.CarbonCommonConstants

object FieldConverter {
  val stringLengthExceedErrorMsg = "Dataload failed, String length cannot exceed "

  /**
   * Return a String representation of the input value
   * @param value input value
   * @param serializationNullFormat string for null value
   * @param complexDelimiters List of Complex Delimiters
   * @param timeStampFormat timestamp format
   * @param dateFormat date format
   * @param isVarcharType whether it is varchar type. A varchar type has no string length limit
   * @param level level for recursive call
   */
  def objectToString(
      value: Any,
      serializationNullFormat: String,
      complexDelimiters: util.ArrayList[String],
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat,
      isVarcharType: Boolean = false,
      isComplexType: Boolean = false,
      level: Int = 0,
      binaryCodec: String
  ): String = {
    if (value == null) {
      serializationNullFormat
    } else {
      value match {
        case s: String => if (!isVarcharType && !isComplexType &&
                              s.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
          throw new IllegalArgumentException(stringLengthExceedErrorMsg +
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
        case bs: Array[Byte] =>
          if ("base64".equalsIgnoreCase(binaryCodec)) {
            // Insert flow is inner flow, the inner binary codec fixed with base64 unify.
            Base64.getEncoder.encodeToString(bs)
          } else {
            new String(bs, Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))
          }
        case s: scala.collection.Seq[Any] =>
          if (s.nonEmpty) {
            val delimiter = complexDelimiters.get(level)
            val builder = new StringBuilder()
            s.foreach { x =>
              val nextLevel = level + 1
              builder.append(objectToString(x, serializationNullFormat, complexDelimiters,
                timeStampFormat, dateFormat, isVarcharType, level = nextLevel,
                binaryCodec = binaryCodec))
                .append(delimiter)
            }
            builder.substring(0, builder.length - delimiter.length())
          } else {
            CarbonCommonConstants.SIZE_ZERO_DATA_RETURN
          }
        // First convert the 'key' of Map and then append the keyValueDelimiter and then convert
        // the 'value of the map and append delimiter
        case m: scala.collection.Map[_, _] =>
          if (m.nonEmpty) {
            val nextLevel = level + 2
            val delimiter = complexDelimiters.get(level)
            val keyValueDelimiter = complexDelimiters.get(level + 1)
            val builder = new StringBuilder()
            m.foreach { x =>
              builder.append(objectToString(x._1, serializationNullFormat, complexDelimiters,
                timeStampFormat, dateFormat, isVarcharType, level = nextLevel,
                binaryCodec = binaryCodec))
                .append(keyValueDelimiter)
              builder.append(objectToString(x._2, serializationNullFormat, complexDelimiters,
                timeStampFormat, dateFormat, isVarcharType, level = nextLevel,
                binaryCodec = binaryCodec))
                .append(delimiter)
            }
            builder.substring(0, builder.length - delimiter.length())
          } else {
            CarbonCommonConstants.SIZE_ZERO_DATA_RETURN
          }
        case r: org.apache.spark.sql.Row =>
          val delimiter = complexDelimiters.get(level)
          val builder = new StringBuilder()
          val len = r.length
          var i = 0
          while (i < len) {
            val nextLevel = level + 1
            builder.append(objectToString(r(i), serializationNullFormat, complexDelimiters,
              timeStampFormat, dateFormat, isVarcharType, level = nextLevel,
              binaryCodec = binaryCodec))
              .append(delimiter)
            i += 1
          }
          builder.substring(0, builder.length - delimiter.length())
        case other => other.toString
      }
    }
  }
}
