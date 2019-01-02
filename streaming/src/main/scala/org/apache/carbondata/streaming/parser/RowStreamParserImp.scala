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

import java.text.SimpleDateFormat
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.processing.loading.ComplexDelimitersEnum
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants

/**
 * SparkSQL Row Stream Parser.
 */
class RowStreamParserImp extends CarbonStreamParser {

  var configuration: Configuration = null
  var structType: StructType = null
  var encoder: ExpressionEncoder[Row] = null

  var timeStampFormat: SimpleDateFormat = null
  var dateFormat: SimpleDateFormat = null
  var complexDelimiters: util.ArrayList[String] = new util.ArrayList[String]()
  var serializationNullFormat: String = null

  override def initialize(configuration: Configuration, structType: StructType): Unit = {
    this.configuration = configuration
    this.structType = structType
    this.encoder = RowEncoder.apply(this.structType).resolveAndBind()

    this.timeStampFormat = new SimpleDateFormat(
      this.configuration.get(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT))
    this.dateFormat = new SimpleDateFormat(
      this.configuration.get(CarbonCommonConstants.CARBON_DATE_FORMAT))
    this.complexDelimiters.add(this.configuration.get("carbon_complex_delimiter_level_1"))
    this.complexDelimiters.add(this.configuration.get("carbon_complex_delimiter_level_2"))
    this.complexDelimiters.add(this.configuration.get("carbon_complex_delimiter_level_3"))
    this.complexDelimiters.add(ComplexDelimitersEnum.COMPLEX_DELIMITERS_LEVEL_4.value())
    this.serializationNullFormat =
      this.configuration.get(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
  }

  override def parserRow(value: InternalRow): Array[Object] = {
    this.encoder.fromRow(value).toSeq.map { x => {
      FieldConverter.objectToString(
        x, serializationNullFormat, complexDelimiters,
        timeStampFormat, dateFormat)
    } }.toArray
  }

  override def close(): Unit = {
  }

}
