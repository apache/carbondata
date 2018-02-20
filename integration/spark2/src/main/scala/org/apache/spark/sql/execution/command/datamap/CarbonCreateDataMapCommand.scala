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

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.preaaggregate.{CreatePreAggregateTableCommand, PreAggregateUtil}
import org.apache.spark.sql.execution.command.timeseries.TimeSeriesUtil
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, MalformedDataMapCommandException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider._
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}

/**
 * Below command class will be used to create datamap on table
 * and updating the parent table about the datamap information
 */
case class CarbonCreateDataMapCommand(
    dataMapName: String,
    tableIdentifier: TableIdentifier,
    dmClassName: String,
    dmproperties: Map[String, String],
    queryString: Option[String])
  extends AtomicRunnableCommand {

  var createPreAggregateTableCommands: CreatePreAggregateTableCommand = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    // since streaming segment does not support building index and pre-aggregate yet,
    // so streaming table does not support create datamap
    val carbonTable =
    CarbonEnv.getCarbonTable(tableIdentifier.database, tableIdentifier.table)(sparkSession)
    if (carbonTable.isStreamingTable) {
      throw new MalformedCarbonCommandException("Streaming table does not support creating datamap")
    }
    validateDataMapName(carbonTable)

    if (dmClassName.equalsIgnoreCase(PREAGGREGATE.toString) ||
      dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      TimeSeriesUtil.validateTimeSeriesGranularity(dmproperties, dmClassName)
      createPreAggregateTableCommands = if (dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
        val details = TimeSeriesUtil
          .getTimeSeriesGranularityDetails(dmproperties, dmClassName)
        val updatedDmProperties = dmproperties - details._1
        CreatePreAggregateTableCommand(dataMapName,
          tableIdentifier,
          dmClassName,
          updatedDmProperties,
          queryString.get,
          Some(details._1))
      } else {
        CreatePreAggregateTableCommand(
          dataMapName,
          tableIdentifier,
          dmClassName,
          dmproperties,
          queryString.get
        )
      }
      try {
        createPreAggregateTableCommands.processMetadata(sparkSession)
      } catch {
        case e: Throwable => throw new MetadataProcessException(s"Failed to create datamap " +
                                                                s"'$dataMapName'", e)
      }
    } else {
      val dataMapSchema = new DataMapSchema(dataMapName, dmClassName)
      dataMapSchema.setProperties(new java.util.HashMap[String, String](dmproperties.asJava))
      val dbName = CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession)
      val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore.lookupRelation(
        Some(dbName),
        tableIdentifier.table)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
      DataMapStoreManager.getInstance().createAndRegisterDataMap(
        carbonTable.getAbsoluteTableIdentifier, dataMapSchema)
      // Save DataMapSchema in the  schema file of main table
      PreAggregateUtil.updateMainTable(carbonTable, dataMapSchema, sparkSession)
    }
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    LOGGER.audit(s"DataMap $dataMapName successfully added to Table ${tableIdentifier.table}")
    Seq.empty
  }

  private def validateDataMapName(carbonTable: CarbonTable) = {
    val existingDataMaps = carbonTable.getTableInfo.getDataMapSchemaList
    existingDataMaps.asScala.foreach { dataMapSchema =>
      if (dataMapSchema.getDataMapName.equalsIgnoreCase(dataMapName)) {
        throw new MalformedDataMapCommandException(s"DataMap name '$dataMapName' already exist")
      }
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dmClassName.equalsIgnoreCase(PREAGGREGATE.toString) ||
      dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      createPreAggregateTableCommands.processData(sparkSession)
    } else {
      Seq.empty
    }
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    if (dmClassName.equalsIgnoreCase(PREAGGREGATE.toString) ||
      dmClassName.equalsIgnoreCase(TIMESERIES.toString)) {
      createPreAggregateTableCommands.undoMetadata(sparkSession, exception)
    } else {
      throw new MalformedDataMapCommandException("Unknown data map type " + dmClassName)
    }
  }
}

