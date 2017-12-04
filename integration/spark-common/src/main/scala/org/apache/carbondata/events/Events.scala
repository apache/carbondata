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
import org.apache.spark.sql.execution.command.{AlterTableDataTypeChangeModel, AlterTableDropColumnModel, AlterTableRenameModel}

import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
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
 * event for alter_table_drop_column
 */
trait AlterTableDropColumnEventInfo {
  val carbonTable: CarbonTable
  val alterTableDropColumnModel: AlterTableDropColumnModel
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
}

/**
 * event for alter_table_rename
 */
trait AlterTableCompactionEventInfo {
  val carbonTable: CarbonTable
  val carbonLoadModel: CarbonLoadModel
  val mergedLoadName: String
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
