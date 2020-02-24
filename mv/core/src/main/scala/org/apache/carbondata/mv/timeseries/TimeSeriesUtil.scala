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

package org.apache.carbondata.mv.timeseries

import scala.util.control.Breaks.{break, breakable}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException, MalformedMaterializedViewException}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Utility class for time series to keep
 */
object TimeSeriesUtil {

  val TIMESERIES_EVENTTIME = "event_time"

  /**
   * Below method will be used to validate whether column mentioned in time series
   * is timestamp column or not
   *
   * @param dmproperties data map properties
   * @param parentTable  parent table
   * @return whether time stamp column
   */
  def validateTimeSeriesEventTime(
      dmproperties: Map[String, String],
      parentTable: CarbonTable) {
    val eventTime = dmproperties.get(TIMESERIES_EVENTTIME)
    if (!eventTime.isDefined) {
      throw new MalformedCarbonCommandException("event_time not defined in time series")
    } else {
      val carbonColumn = parentTable.getColumnByName(eventTime.get.trim)
      if (carbonColumn.getDataType != DataTypes.TIMESTAMP) {
        throw new MalformedCarbonCommandException(
          "Timeseries event time is only supported on Timestamp column")
      }
    }
  }

  /**
   * validate TimeSeries Granularity
   *
   * @param dmProperties datamap properties
   * @param dmClassName  datamap class name
   * @return whether find  only one granularity
   */
  def validateTimeSeriesGranularity(
      dmProperties: java.util.Map[String, String],
      dmClassName: String): Boolean = {
    var isFound = false

    // 1. granularity only support one
    for (granularity <- Granularity.values()) {
      if (dmProperties.containsKey(granularity.getName)) {
        if (isFound) {
          throw new MalformedMaterializedViewException(
            s"Only one granularity level can be defined")
        } else {
          isFound = true
        }
      }
    }
    isFound
  }

  /**
   * get TimeSeries Granularity key and value
   * check the value
   *
   * TODO:we will support value not only equal to 1 in the future
   *
   * @param dmProperties datamap properties
   * @param dmClassName  datamap class name
   * @return key and value tuple
   */
  def getTimeSeriesGranularityDetails(
      dmProperties: java.util.Map[String, String],
      dmClassName: String): (String, String) = {

    val defaultValue = "1"
    for (granularity <- Granularity.values()) {
      if (dmProperties.containsKey(granularity.getName) &&
        dmProperties.get(granularity.getName).trim.equalsIgnoreCase(defaultValue)) {
        return (granularity.toString.toLowerCase, dmProperties.get(granularity.getName))
      }
    }

    throw new MalformedMaterializedViewException(
      s"Granularity only support $defaultValue")
  }

  def validateTimeSeriesGranularityForDate(
      timeSeriesFunction: String): Unit = {
    for (granularity <- Granularity.values()) {
      if (timeSeriesFunction.equalsIgnoreCase(granularity.getName
        .substring(0, granularity.getName.lastIndexOf(CarbonCommonConstants.UNDERSCORE)))) {
        if (!supportedGranularitiesForDate.contains(granularity.getName)) {
          throw new MalformedCarbonCommandException(
            "Granularity should be of DAY/WEEK/MONTH/YEAR, for timeseries column of Date type")
        }
      }
    }
  }

  val supportedGranularitiesForDate: Seq[String] = Seq(
    Granularity.DAY.getName,
    Granularity.WEEK.getName,
    Granularity.MONTH.getName,
    Granularity.YEAR.getName)

  /**
   * validate TimeSeries Granularity
   *
   * @param timeSeriesFunction user defined granularity
   */
  def validateTimeSeriesGranularity(
      timeSeriesFunction: String): Unit = {
    var found = false
    breakable {
      for (granularity <- Granularity.values()) {
        if (timeSeriesFunction.equalsIgnoreCase(granularity.getName
          .substring(0, granularity.getName.lastIndexOf(CarbonCommonConstants.UNDERSCORE)))) {
          found = true
          break
        }
      }
    }
    if (!found) {
      throw new MalformedCarbonCommandException(
        "Granularity " + timeSeriesFunction + " is invalid")
    }
  }

}
