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

import org.apache.spark.sql.execution.command.{DataMapField, Field}

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider.TIMESERIES
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
      val carbonColumn = parentTable.getColumnByName(parentTable.getTableName, eventTime.get)
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
        dmProperties.get(granularity.getName).equalsIgnoreCase(defaultValue)) {
        return (granularity.toString.toLowerCase, dmProperties.get(granularity.getName))
      }
    }

    throw new MalformedDataMapCommandException(
      s"Granularity only support $defaultValue")
  }

  /**
   * Below method will be used to validate the hierarchy of time series and its value
   * validation will be done whether hierarchy order is proper or not and hierarchy level
   * value
   * TODO: we should remove this method
   *
   * @param timeSeriesHierarchyDetails
   * time series hierarchy string
   */
  @deprecated
  def validateAndGetTimeSeriesHierarchyDetails(timeSeriesHierarchyDetails: String): Array[
    (String, String)] = {
    val updatedtimeSeriesHierarchyDetails = timeSeriesHierarchyDetails.toLowerCase
    val timeSeriesHierarchy = updatedtimeSeriesHierarchyDetails.split(",")
    val hierBuffer = timeSeriesHierarchy.map {
      case f =>
        val splits = f.split("=")
        // checking hierarchy name is valid or not
        if (!TimeSeriesUDF.INSTANCE.TIMESERIES_FUNCTION.contains(splits(0).toLowerCase)) {
          throw new MalformedCarbonCommandException(s"Not supported heirarchy type: ${ splits(0) }")
        }
        // validating hierarchy level is valid or not
        if (!splits(1).equals("1")) {
          throw new MalformedCarbonCommandException(
            s"Unsupported Value for hierarchy:" +
            s"${ splits(0) }=${ splits(1) }")
        }
        (splits(0), splits(1))
    }
    // checking whether hierarchy is in proper order or not
    // get the index of first hierarchy
    val indexOfFirstHierarchy = TimeSeriesUDF.INSTANCE.TIMESERIES_FUNCTION
      .indexOf(hierBuffer(0)._1.toLowerCase)
    val index = 0
    // now iterating through complete hierarchy to check any of the hierarchy index
    // is less than first one
    for (index <- 1 to hierBuffer.size - 1) {
      val currentIndex = TimeSeriesUDF.INSTANCE.TIMESERIES_FUNCTION
        .indexOf(hierBuffer(index)._1.toLowerCase)
      if (currentIndex < indexOfFirstHierarchy) {
        throw new MalformedCarbonCommandException(s"$timeSeriesHierarchyDetails is in wrong order")
      }
    }
    hierBuffer
  }

  /**
   * Below method will be used to validate whether timeseries column present in
   * select statement or not
   * @param fieldMapping
   *                     fields from select plan
   * @param timeSeriesColumn
   *                         timeseries column name
   */
  def validateEventTimeColumnExitsInSelect(fieldMapping: scala.collection.mutable
  .LinkedHashMap[Field, DataMapField],
      timeSeriesColumn: String) : Any = {
    val isTimeSeriesColumnExits = fieldMapping
      .exists(obj => obj._2.columnTableRelationList.isDefined &&
                     obj._2.columnTableRelationList.get(0).parentColumnName
                       .equalsIgnoreCase(timeSeriesColumn) &&
                     obj._2.aggregateFunction.isEmpty)
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
  def updateTimeColumnSelect(fieldMapping: scala.collection.mutable
  .LinkedHashMap[Field, DataMapField],
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

