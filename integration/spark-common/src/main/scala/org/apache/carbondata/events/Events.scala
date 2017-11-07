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

import org.apache.spark.sql.execution.command.{AlterTableDropColumnModel, AlterTableRenameModel}

import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.Event
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
  * event for database operations
  */
trait DatabaseEvent extends Event {
  val databaseName: String
}

/**
  * event for table related operations
  */
trait TableEvent extends DatabaseEvent {
  val carbonTableIdentifier: CarbonTableIdentifier
  override lazy val databaseName: String = carbonTableIdentifier.getDatabaseName
}

/**
  * event for load operations
  */
trait LoadEvent extends TableEvent {
  val carbonLoadModel: CarbonLoadModel
}

/**
  * event for lookup
  */
trait LookupRelationEvent extends TableEvent {
  val carbonTable: CarbonTable
  override val carbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}


/**
  * event for drop table
  */
trait DropTableEvent extends TableEvent {
  val carbonTable: CarbonTable
  val ifExistsSet: Boolean
  override val carbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for alter_table_drop_column
  */
trait AlterTableDropColumnEvent extends TableEvent {
  val carbonTable: CarbonTable
  val alterTableDropColumnModel: AlterTableDropColumnModel
  override val carbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for alter_table_rename
  */
trait AlterTableRenameEvent extends TableEvent {
  val carbonTable: CarbonTable
  val alterTableRenameModel: AlterTableRenameModel
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for alter_table_rename
  */
trait AlterTableCompactionEvent extends TableEvent {
  val carbonTable: CarbonTable
  val carbonLoadModel: CarbonLoadModel
  val mergedLoadName: String
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for DeleteSegmentById
  */
trait DeleteSegmentbyIdEvent extends TableEvent {
  val carbonTable: CarbonTable
  val loadIds: Seq[String]
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for DeleteSegmentByDate
  */
trait DeleteSegmentbyDateEvent extends TableEvent {
  val carbonTable: CarbonTable
  val loadDates: String
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for Clean Files
  */
trait CleanFilesEvent extends TableEvent {
  val carbonTable: CarbonTable
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for update table
  */
trait UpdateTableEvent extends TableEvent {
  val carbonTable: CarbonTable
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}

/**
  * event for delete from table
  */
trait DeleteFromTableEvent extends TableEvent {
  val carbonTable: CarbonTable
  override val carbonTableIdentifier: CarbonTableIdentifier = carbonTable.getCarbonTableIdentifier
}
