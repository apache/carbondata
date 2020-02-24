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
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel, AlterTableDropColumnModel, AlterTableRenameModel, CarbonMergerMapping}

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * event for database operations
 */
trait DatabaseEventInfo {
  val databaseName: String
}

/**
 * event for table related operations
 */
trait TableEventInfo {
  val identifier: AbsoluteTableIdentifier
}

/**
 * event for load operations
 */
trait LoadEventInfo {
  val carbonLoadModel: CarbonLoadModel
}

/**
 * event for lookup
 */
trait LookupRelationEventInfo {
  val carbonTable: CarbonTable
}


/**
 * event for drop table
 */
trait DropTableEventInfo {
  val carbonTable: CarbonTable
  val ifExistsSet: Boolean
}

/**
 * event for show cache
 */
trait ShowTableCacheEventInfo {
  val carbonTable: CarbonTable
}

/**
 * event for drop cache
 */
trait DropTableCacheEventInfo {
  val carbonTable: CarbonTable
}

/**
 * event for alter_table_drop_column
 */
trait AlterTableDropColumnEventInfo {
  val carbonTable: CarbonTable
  val alterTableDropColumnModel: AlterTableDropColumnModel
}

trait AlterTableDropPartitionEventInfo {
  val parentCarbonTable: CarbonTable
  val specs: Seq[TablePartitionSpec]
  val ifExists: Boolean
  val purge: Boolean
  val retainData: Boolean
}

trait AlterTableDataTypeChangeEventInfo {
  val carbonTable: CarbonTable
  val alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel
}

/**
 * event for alter_table_rename
 */
trait AlterTableRenameEventInfo {
  val carbonTable: CarbonTable
  val alterTableRenameModel: AlterTableRenameModel
}

/**
 * event for alter_add_column
 */
trait AlterTableAddColumnEventInfo {
  val carbonTable: CarbonTable
  val alterTableAddColumnsModel: AlterTableAddColumnsModel
}

/**
 * event for alter_table_rename
 */
trait AlterTableCompactionStatusUpdateEventInfo {
  val carbonTable: CarbonTable
  val carbonMergerMapping: CarbonMergerMapping
  val mergedLoadName: String
}

/**
 * event info for alter_table_compaction
 */
trait AlterTableCompactionEventInfo {
  val sparkSession: SparkSession
  val carbonTable: CarbonTable
}

/**
 * event for alter table standard hive partition
 */
trait AlterTableHivePartitionInfo {
  val sparkSession: SparkSession
  val carbonTable: CarbonTable
}

/**
 * event for DeleteSegmentById
 */
trait DeleteSegmentbyIdEventInfo {
  val carbonTable: CarbonTable
  val loadIds: Seq[String]
}

/**
 * event for DeleteSegmentByDate
 */
trait DeleteSegmentbyDateEventInfo {
  val carbonTable: CarbonTable
  val loadDates: String
}

/**
 * event for Clean Files
 */
trait CleanFilesEventInfo {
  val carbonTable: CarbonTable
}

/**
 * event for update table
 */
trait UpdateTableEventInfo {
  val carbonTable: CarbonTable
}

/**
 * event for delete from table
 */
trait DeleteFromTableEventInfo {
  val carbonTable: CarbonTable
}

/**
 * event to initiate CarbonEnv
 */
trait SessionEventInfo {
  val sparkSession: SparkSession
}

/**
 * Event for lookup
 */
trait CreateCarbonRelationEventInfo {
  val sparkSession: SparkSession
  val carbonTable: CarbonTable
}

/**
 * Event info for create datamap
 */
trait CreateDataMapEventsInfo {
  val sparkSession: SparkSession
  val storePath: String
}

/**
 * Event info for build datamap
 */
trait BuildDataMapEventsInfo {
  val sparkSession: SparkSession
  val identifier: AbsoluteTableIdentifier
  val dataMapNames: scala.collection.mutable.Seq[String]
}

/**
 * EventInfo for prepriming on IndexServer. This event is used to
 * fire a call to the index serevr when the load is complete.
 */
trait IndexServerEventInfo {
  val carbonTable: CarbonTable
  val sparkSession: SparkSession
}
