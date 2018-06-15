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
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.parser.CarbonStreamParser

class StreamingOption(val userInputMap: Map[String, String]) {
  def trigger: Trigger = {
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

  def timeStampFormat: String =
    userInputMap.getOrElse("timestampformat", CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  def dateFormat: String =
    userInputMap.getOrElse("dateformat", CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

  def rowParser: String =
    userInputMap.getOrElse(CarbonStreamParser.CARBON_STREAM_PARSER,
      CarbonStreamParser.CARBON_STREAM_PARSER_ROW_PARSER)

  def remainingOption: Map[String, String] = {
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
