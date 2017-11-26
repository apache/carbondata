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

package org.apache.spark.sql.execution.command.schema

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDropColumnModel, RunnableCommand}
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionState}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.events.{AlterTableDropColumnPostEvent, AlterTableDropColumnPreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.SchemaEvolutionEntry
import org.apache.carbondata.spark.rdd.AlterTableDropColumnRDD

private[sql] case class CarbonAlterTableDropColumnCommand(
    alterTableDropColumnModel: AlterTableDropColumnModel)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableDropColumnModel.tableName
    val dbName = alterTableDropColumnModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table drop columns request has been received for $dbName.$tableName")
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    // get the latest carbon table and check for column existence
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      val partitionInfo = carbonTable.getPartitionInfo(tableName)
      if (partitionInfo != null) {
        val partitionColumnSchemaList = partitionInfo.getColumnSchemaList.asScala
          .map(_.getColumnName)
        // check each column existence in the table
        val partitionColumns = alterTableDropColumnModel.columns.filter {
          tableColumn => partitionColumnSchemaList.contains(tableColumn)
        }
        if (partitionColumns.nonEmpty) {
          throw new UnsupportedOperationException("Partition columns cannot be dropped: " +
                                                  s"$partitionColumns")
        }
      }
      val tableColumns = carbonTable.getCreateOrderColumn(tableName).asScala
      var dictionaryColumns = Seq[org.apache.carbondata.core.metadata.schema.table.column
      .ColumnSchema]()
      var keyColumnCountToBeDeleted = 0
      // TODO: if deleted column list includes bucketted column throw an error
      alterTableDropColumnModel.columns.foreach { column =>
        var columnExist = false
        tableColumns.foreach { tableColumn =>
          // column should not be already deleted and should exist in the table
          if (!tableColumn.isInvisible && column.equalsIgnoreCase(tableColumn.getColName)) {
            if (tableColumn.isDimension) {
              keyColumnCountToBeDeleted += 1
              if (tableColumn.hasEncoding(Encoding.DICTIONARY)) {
                dictionaryColumns ++= Seq(tableColumn.getColumnSchema)
              }
            }
            columnExist = true
          }
        }
        if (!columnExist) {
          sys.error(s"Column $column does not exists in the table $dbName.$tableName")
        }
      }
      // take the total key column count. key column to be deleted should not
      // be >= key columns in schema
      val totalKeyColumnInSchema = tableColumns.count {
        tableColumn => !tableColumn.isInvisible && tableColumn.isDimension
      }
      if (keyColumnCountToBeDeleted >= totalKeyColumnInSchema) {
        sys.error(s"Alter drop operation failed. AtLeast one key column should exist after drop.")
      }

      val operationContext = new OperationContext
      // event will be fired before dropping the columns
      val alterTableDropColumnPreEvent: AlterTableDropColumnPreEvent = AlterTableDropColumnPreEvent(
        carbonTable,
        alterTableDropColumnModel,
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableDropColumnPreEvent, operationContext)

      // read the latest schema file
      val carbonTablePath = CarbonStorePath
        .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
      val tableInfo: org.apache.carbondata.format.TableInfo =
        metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
      // maintain the deleted columns for schema evolution history
      var deletedColumnSchema = ListBuffer[org.apache.carbondata.format.ColumnSchema]()
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala
      alterTableDropColumnModel.columns.foreach { column =>
        columnSchemaList.foreach { columnSchema =>
          if (!columnSchema.invisible && column.equalsIgnoreCase(columnSchema.column_name)) {
            deletedColumnSchema += columnSchema.deepCopy
            columnSchema.invisible = true
          }
        }
      }
      // add deleted columns to schema evolution history and update the schema
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new SchemaEvolutionEntry(timeStamp)
      schemaEvolutionEntry.setRemoved(deletedColumnSchema.toList.asJava)
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaEvolutionEntry,
          tableInfo)(sparkSession,
          sparkSession.sessionState.asInstanceOf[CarbonSessionState])
      // TODO: 1. add check for deletion of index tables
      // delete dictionary files for dictionary column and clear dictionary cache from memory
      new AlterTableDropColumnRDD(sparkSession.sparkContext,
        dictionaryColumns,
        carbonTable.getAbsoluteTableIdentifier).collect()

      // event will be fired before dropping the columns
      val alterTableDropColumnPostEvent: AlterTableDropColumnPostEvent =
        AlterTableDropColumnPostEvent(
        carbonTable,
        alterTableDropColumnModel,
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableDropColumnPostEvent, operationContext)

      LOGGER.info(s"Alter table for drop columns is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for drop columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception => LOGGER
        .error("Alter table drop columns failed : " + e.getMessage)
        if (carbonTable != null) {
          AlterTableUtil.revertDropColumnChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        sys.error(s"Alter table drop column operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }
}
