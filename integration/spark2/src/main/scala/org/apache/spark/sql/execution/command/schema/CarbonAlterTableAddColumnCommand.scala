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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableColumnSchemaGenerator, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.apache.spark.util.{AlterTableUtil, SparkUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{AlterTableAddColumnPostEvent, AlterTableAddColumnPreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.TableInfo
import org.apache.carbondata.spark.rdd.{AlterTableAddColumnRDD, AlterTableDropColumnRDD}

private[sql] case class CarbonAlterTableAddColumnCommand(
    alterTableAddColumnsModel: AlterTableAddColumnsModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableAddColumnsModel.tableName
    val dbName = alterTableAddColumnsModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    setAuditTable(dbName, tableName)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    var newCols = Seq[org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema]()
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      // Consider a concurrent scenario where 2 alter operations are executed in parallel. 1st
      // operation is success and updates the schema file. 2nd operation will get the lock after
      // completion of 1st operation but as look up relation is called before it will have the
      // older carbon table and this can lead to inconsistent state in the system. Therefor look
      // up relation should be called after acquiring the lock
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      if (!carbonTable.canAllow(carbonTable, TableOperation.ALTER_ADD_COLUMN)) {
        throw new MalformedCarbonCommandException(
          "alter table add column is not supported for index datamap")
      }
      val operationContext = new OperationContext
      val alterTableAddColumnListener = AlterTableAddColumnPreEvent(sparkSession, carbonTable,
        alterTableAddColumnsModel)
      OperationListenerBus.getInstance().fireEvent(alterTableAddColumnListener, operationContext)
      // get the latest carbon table and check for column existence
      // read the latest schema file
      val thriftTableInfo: TableInfo = metastore.getThriftTableInfo(carbonTable)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(thriftTableInfo,
          dbName,
          tableName,
          carbonTable.getTablePath)
      newCols = new AlterTableColumnSchemaGenerator(alterTableAddColumnsModel,
        dbName,
        wrapperTableInfo,
        carbonTable.getAbsoluteTableIdentifier,
        sparkSession.sparkContext).process
      setAuditInfo(Map(
        "newColumn" -> newCols.map(x => s"${x.getColumnName}:${x.getDataType}").mkString(",")))
      // generate dictionary files for the newly added columns
      new AlterTableAddColumnRDD(sparkSession,
        newCols,
        carbonTable.getAbsoluteTableIdentifier).collect()
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
      schemaEvolutionEntry.setTimeStamp(timeStamp)
      schemaEvolutionEntry.setAdded(newCols.toList.asJava)
      val thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
      // carbon columns based on schema order
      val carbonColumns = carbonTable.getCreateOrderColumn(carbonTable.getTableName).asScala
        .collect { case carbonColumn if !carbonColumn.isInvisible => carbonColumn.getColumnSchema }
      // sort the new columns based on schema order
      val sortedColsBasedActualSchemaOrder = newCols.sortBy(a => a.getSchemaOrdinal)
      val (tableIdentifier, schemaParts) = AlterTableUtil.updateSchemaInfo(
          carbonTable,
          schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry),
          thriftTable)(sparkSession)
      // In case of spark2.2 and above and , when we call
      // alterExternalCatalogForTableWithUpdatedSchema to update the new schema to external catalog
      // in case of add column, spark gets the catalog table and then it itself adds the partition
      // columns if the table is partition table for all the new data schema sent by carbon,
      // so there will be duplicate partition columns, so send the columns without partition columns
      val cols = if (SparkUtil.isSparkVersionXandAbove("2.2") && carbonTable.isHivePartitionTable) {
        val partitionColumns = carbonTable.getPartitionInfo.getColumnSchemaList.asScala
        val carbonColumnsWithoutPartition = carbonColumns.filterNot(col => partitionColumns.contains
        (col))
        Some(carbonColumnsWithoutPartition ++ sortedColsBasedActualSchemaOrder)
      } else {
        Some(carbonColumns ++ sortedColsBasedActualSchemaOrder)
      }
      CarbonSessionCatalogUtil.alterAddColumns(tableIdentifier, schemaParts, cols, sparkSession)
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
      val alterTablePostExecutionEvent: AlterTableAddColumnPostEvent =
        AlterTableAddColumnPostEvent(sparkSession, carbonTable, alterTableAddColumnsModel)
      OperationListenerBus.getInstance.fireEvent(alterTablePostExecutionEvent, operationContext)
      LOGGER.info(s"Alter table for add columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        if (newCols.nonEmpty) {
          LOGGER.info("Cleaning up the dictionary files as alter table add operation failed")
          new AlterTableDropColumnRDD(sparkSession,
            newCols,
            carbonTable.getAbsoluteTableIdentifier).collect()
          AlterTableUtil.revertAddColumnChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        throwMetadataException(dbName, tableName,
          s"Alter table add operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }

  override protected def opName: String = "ALTER TABLE ADD COLUMN"
}
