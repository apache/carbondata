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
package org.apache.spark.sql.execution.command.datamap

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.preaaggregate.CreatePreAggregateTableCommand
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider._
import org.apache.carbondata.spark.exception.{MalformedCarbonCommandException, MalformedDataMapCommandException}

/**
 * Below command class will be used to create datamap on table
 * and updating the parent table about the datamap information
 */
case class CarbonCreateDataMapCommand(
    dataMapName: String,
    tableIdentifier: TableIdentifier,
    dmClassName: String,
    dmProperties: Map[String, String],
    queryString: Option[String],
    ifNotExistsSet: Boolean = false)
  extends AtomicRunnableCommand {

  var createPreAggregateTableCommands: CreatePreAggregateTableCommand = _
  var tableIsExists: Boolean = false

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index and pre-aggregate yet,
    // so streaming table does not support create datamap
    val carbonTable =
    CarbonEnv.getCarbonTable(tableIdentifier.database, tableIdentifier.table)(sparkSession)
    if (carbonTable.isStreamingTable) {
      throw new MalformedCarbonCommandException("Streaming table does not support creating datamap")
    }
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = tableIdentifier.database.getOrElse("default")
    val tableName = tableIdentifier.table + "_" + dataMapName
    val newDmProperties = if (dmProperties.get(TimeSeriesUtil.TIMESERIES_EVENTTIME).isDefined) {
      dmProperties.updated(TimeSeriesUtil.TIMESERIES_EVENTTIME,
        dmProperties.get(TimeSeriesUtil.TIMESERIES_EVENTTIME).get.trim)
    } else {
      dmProperties
    }
    val dataMapProvider = {
      try {
        DataMapProvider.getDataMapProvider(dmClassName)
      } catch {
        case e: UnsupportedOperationException =>
          throw new MalformedDataMapCommandException(e.getMessage)
      }
    }
    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      LOGGER.audit(
        s"Table creation with Database name [$dbName] and Table name [$tableName] failed. " +
          s"Table [$tableName] already exists under database [$dbName]")
      tableIsExists = true
      if (!ifNotExistsSet) {
        throw new TableAlreadyExistsException(dbName, tableName)
      }
    } else {
      TimeSeriesUtil.validateTimeSeriesGranularity(newDmProperties, dmClassName)
      createPreAggregateTableCommands = if (dataMapProvider == TIMESERIES) {
        val details = TimeSeriesUtil
          .getTimeSeriesGranularityDetails(newDmProperties, dmClassName)
        val updatedDmProperties = newDmProperties - details._1
        CreatePreAggregateTableCommand(
          dataMapName,
          tableIdentifier,
          dataMapProvider,
          updatedDmProperties,
          queryString.get,
          Some(details._1),
          ifNotExistsSet = ifNotExistsSet)
      } else {
        CreatePreAggregateTableCommand(
          dataMapName,
          tableIdentifier,
          dataMapProvider,
          newDmProperties,
          queryString.get,
          ifNotExistsSet = ifNotExistsSet)
      }
      createPreAggregateTableCommands.processMetadata(sparkSession)
    }
    LOGGER.audit(s"DataMap $dataMapName successfully added to Table ${tableIdentifier.table}")
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dmClassName.equalsIgnoreCase(PREAGGREGATE.toString) ||
      dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      if (!tableIsExists) {
        createPreAggregateTableCommands.processData(sparkSession)
      } else {
        Seq.empty
      }
    } else {
      throw new MalformedDataMapCommandException("Unknown datamap provider/class " + dmClassName)
    }
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (dmClassName.equalsIgnoreCase(PREAGGREGATE.toString) ||
      dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      if (!tableIsExists && createPreAggregateTableCommands != null) {
        createPreAggregateTableCommands.undoMetadata(sparkSession, exception)
      } else {
        Seq.empty
      }
    } else {
      throw new MalformedDataMapCommandException("Unknown datamap provider/class " + dmClassName)
    }
  }
}

