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
package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command._

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 *
 * @param carbonTable
 * @param alterTableDropColumnModel
 * @param sparkSession
 */
case class AlterTableDropColumnPreEvent(
    carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends Event with AlterTableDropColumnEventInfo


/**
 * Class for handling clean up in case of any failure and abort the operation
 *
 * @param carbonTable
 * @param alterTableDataTypeChangeModel
 */
case class AlterTableColRenameAndDataTypeChangePreEvent(
    sparkSession: SparkSession,
    carbonTable: CarbonTable,
        alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel)
  extends Event with AlterTableDataTypeChangeEventInfo

/**
 * Class for handling clean up in case of any failure and abort the operation
 *
 * @param carbonTable
 * @param alterTableDataTypeChangeModel
 */
case class AlterTableColRenameAndDataTypeChangePostEvent(
    sparkSession: SparkSession,
    carbonTable: CarbonTable,
    alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel)
  extends Event with AlterTableDataTypeChangeEventInfo

/**
 *
 * @param carbonTable
 * @param alterTableDropColumnModel
 * @param sparkSession
 */
case class AlterTableDropColumnPostEvent(
    carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends Event with AlterTableDropColumnEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableDropColumnModel
 * @param sparkSession
 */
case class AlterTableDropColumnAbortEvent(
    carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends Event with AlterTableDropColumnEventInfo

/**
 *
 * @param carbonTable
 * @param alterTableRenameModel
 * @param newTablePath
 * @param sparkSession
 */
case class AlterTableRenamePreEvent(
    carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends Event with AlterTableRenameEventInfo

/**
 *
 * @param carbonTable
 * @param alterTableAddColumnsModel
 */
case class AlterTableAddColumnPreEvent(
    sparkSession: SparkSession,
    carbonTable: CarbonTable,
    alterTableAddColumnsModel: AlterTableAddColumnsModel)
  extends Event with AlterTableAddColumnEventInfo

/**
 *
 * @param carbonTable
 * @param alterTableAddColumnsModel
 */
case class AlterTableAddColumnPostEvent(
    sparkSession: SparkSession,
    carbonTable: CarbonTable,
    alterTableAddColumnsModel: AlterTableAddColumnsModel)
  extends Event with AlterTableAddColumnEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableRenameModel
 * @param newTablePath
 * @param sparkSession
 */
case class AlterTableRenamePostEvent(
    carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends Event with AlterTableRenameEventInfo


/**
 *
 * @param carbonTable
 * @param alterTableRenameModel
 * @param newTablePath
 * @param sparkSession
 */
case class AlterTableRenameAbortEvent(
    carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends Event with AlterTableRenameEventInfo


/**
 * Event for handling pre compaction operations, lister has to implement this event on pre execution
 *
 * @param sparkSession
 * @param carbonTable
 */
case class AlterTableCompactionPreEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    carbonMergerMapping: CarbonMergerMapping,
    mergedLoadName: String) extends Event with AlterTableCompactionEventInfo

/**
 * Compaction Event for handling pre update status file opeartions, lister has to implement this
 * event before updating the table status file
 * @param sparkSession
 * @param carbonTable
 * @param carbonMergerMapping
 */
case class AlterTableCompactionPostEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    carbonMergerMapping: CarbonMergerMapping,
    compactedLoads: java.util.List[String]) extends Event with AlterTableCompactionEventInfo
/**
 * Compaction Event for handling pre update status file opeartions, lister has to implement this
 * event before updating the table status file
 * @param sparkSession
 * @param carbonTable
 * @param carbonMergerMapping
 * @param carbonLoadModel
 * @param mergedLoadName
 */
case class AlterTableCompactionPreStatusUpdateEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    carbonMergerMapping: CarbonMergerMapping,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String) extends Event with AlterTableCompactionStatusUpdateEventInfo

/**
 * Compaction Event for handling post update status file operations, like committing child
 * datamaps in one transaction
 */
case class AlterTableCompactionPostStatusUpdateEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    carbonMergerMapping: CarbonMergerMapping,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String) extends Event with AlterTableCompactionStatusUpdateEventInfo

/**
 * Compaction Event for handling clean up in case of any compaction failure and abort the
 * * operation, lister has to implement this event to handle failure scenarios
 *
 * @param sparkSession
 * @param carbonTable
 * @param alterTableModel
 */
case class AlterTableCompactionAbortEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    alterTableModel: AlterTableModel) extends Event with AlterTableCompactionEventInfo

/**
 * Compaction Event for handling merge index in alter DDL
 *
 * @param sparkSession spark session
 * @param carbonTable carbon table
 * @param alterTableModel alter request
 */
case class AlterTableMergeIndexEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    alterTableModel: AlterTableModel) extends Event with AlterTableCompactionEventInfo

/**
 * pre event for standard hive partition
 * @param sparkSession
 * @param carbonTable
 */
case class PreAlterTableHivePartitionCommandEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable) extends Event with AlterTableHivePartitionInfo

/**
 * post event for standard hive partition
 * @param sparkSession
 * @param carbonTable
 */
case class PostAlterTableHivePartitionCommandEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable) extends Event with AlterTableHivePartitionInfo

case class AlterTableDropPartitionMetaEvent(parentCarbonTable: CarbonTable,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    retainData: Boolean)
  extends Event with AlterTableDropPartitionEventInfo

case class AlterTableDropPartitionPreStatusEvent(carbonTable: CarbonTable) extends Event

case class AlterTableDropPartitionPostStatusEvent(carbonTable: CarbonTable) extends Event
