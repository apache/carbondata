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

package org.apache.carbondata.spark

import scala.collection.mutable

import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser

class StreamingOption(val userInputMap: Map[String, String]) {
  lazy val trigger: Trigger = {
    val trigger = userInputMap.getOrElse(
      "trigger", throw new MalformedCarbonCommandException("trigger must be specified"))
    val interval = userInputMap.getOrElse(
      "interval", throw new MalformedCarbonCommandException("interval must be specified"))
    trigger match {
      case "ProcessingTime" => ProcessingTime(interval)
      case others => throw new MalformedCarbonCommandException("invalid trigger: " + trigger)
    }
  }

  def checkpointLocation(tablePath: String): String =
    userInputMap.getOrElse(
      "checkpointLocation",
      CarbonTablePath.getStreamingCheckpointDir(tablePath))

  lazy val timeStampFormat: String =
    userInputMap.getOrElse("timestampformat", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  lazy val dateFormat: String =
    userInputMap.getOrElse("dateformat", CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

  lazy val rowParser: String =
    userInputMap.getOrElse(CarbonStreamParser.CARBON_STREAM_PARSER,
      CarbonStreamParser.CARBON_STREAM_PARSER_ROW_PARSER)

  lazy val badRecordsPath: String =
    userInputMap
      .getOrElse("bad_record_path", CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC,
          CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL))

  lazy val badRecordsAction: String =
    userInputMap
      .getOrElse("bad_records_action", CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION,
          CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION_DEFAULT))

  lazy val badRecordsLogger: String =
    userInputMap
      .getOrElse("bad_records_logger_enable", CarbonProperties.getInstance()
        .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE,
          CarbonLoadOptionConstants.CARBON_OPTIONS_BAD_RECORDS_LOGGER_ENABLE_DEFAULT))

  lazy val isEmptyBadRecord: String =
    userInputMap
      .getOrElse("is_empty_bad_record", CarbonProperties.getInstance()
        .getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD,
          CarbonLoadOptionConstants.CARBON_OPTIONS_IS_EMPTY_DATA_BAD_RECORD_DEFAULT))

  lazy val remainingOption: Map[String, String] = {
    // copy the user input map and remove the fix options
    val mutableMap = mutable.Map[String, String]() ++= userInputMap
    mutableMap.remove("checkpointLocation")
    mutableMap.remove("timestampformat")
    mutableMap.remove("dateformat")
    mutableMap.remove("trigger")
    mutableMap.remove("interval")
    mutableMap.remove(CarbonStreamParser.CARBON_STREAM_PARSER)
    mutableMap.toMap
  }
}
