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
package org.apache.spark.sql.execution.command.timeseries

import scala.collection.mutable

import org.apache.spark.sql.execution.command.{DataMapField, Field}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.TIMESERIES
import org.apache.carbondata.core.metadata.schema.datamap.Granularity
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.preagg.TimeSeriesUDF

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
  def validateTimeSeriesEventTime(dmproperties: Map[String, String],
      parentTable: CarbonTable) {
    val eventTime = dmproperties.get(TIMESERIES_EVENTTIME)
    if (!eventTime.isDefined) {
      throw new MalformedCarbonCommandException("event_time not defined in time series")
    } else {
      val carbonColumn = parentTable.getColumnByName(parentTable.getTableName, eventTime.get.trim)
      if (carbonColumn.getDataType != DataTypes.TIMESTAMP) {
        throw new MalformedCarbonCommandException(
          "Timeseries event time is only supported on Timestamp " +
          "column")
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
          throw new MalformedDataMapCommandException(
            s"Only one granularity level can be defined")
        } else {
          isFound = true
        }
      }
    }

    // 2. check whether timeseries and granularity match
    if (isFound && !dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      throw new MalformedDataMapCommandException(
        s"${TIMESERIES.toString} keyword missing")
    } else if (!isFound && dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      throw new MalformedDataMapCommandException(
        s"${TIMESERIES.toString} should define time granularity")
    } else if (isFound) {
      true
    } else {
      false
    }
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

    throw new MalformedDataMapCommandException(
      s"Granularity only support $defaultValue")
  }

  /**
   * Below method will be used to validate whether timeseries column present in
   * select statement or not
   *
   * @param fieldMapping     fields from select plan
   * @param timeSeriesColumn timeseries column name
   */
  def validateEventTimeColumnExitsInSelect(
      fieldMapping: mutable.LinkedHashMap[Field, DataMapField],
      timeSeriesColumn: String) : Any = {
    val isTimeSeriesColumnExits = fieldMapping.exists { case (_, f) =>
      f.columnTableRelationList.isDefined &&
      f.columnTableRelationList.get.head.parentColumnName.equalsIgnoreCase(timeSeriesColumn) &&
      f.aggregateFunction.isEmpty
    }
    if(!isTimeSeriesColumnExits) {
      throw new MalformedCarbonCommandException(s"Time series column ${ timeSeriesColumn } does " +
                                                s"not exists in select")
    }
  }

  /**
   * Below method will be used to validate whether timeseries column present in
   * select statement or not
   * @param fieldMapping
   *                     fields from select plan
   * @param timeSeriesColumn
   *                         timeseries column name
   */
  def updateTimeColumnSelect(
      fieldMapping: scala.collection.mutable.LinkedHashMap[Field, DataMapField],
      timeSeriesColumn: String,
      timeSeriesFunction: String) : Any = {
    val isTimeSeriesColumnExits = fieldMapping
      .find(obj => obj._2.columnTableRelationList.isDefined &&
                     obj._2.columnTableRelationList.get(0).parentColumnName
                       .equalsIgnoreCase(timeSeriesColumn) &&
                     obj._2.aggregateFunction.isEmpty)
    isTimeSeriesColumnExits.get._2.aggregateFunction = timeSeriesFunction
  }
}

