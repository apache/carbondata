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

import java.util

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.execution.command.{AlterTableRenameModel, MetadataCommand}
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionCatalogUtil}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events.{AlterTableRenamePostEvent, AlterTableRenamePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.SchemaEvolutionEntry

private[sql] case class CarbonAlterTableRenameCommand(
    alterTableRenameModel: AlterTableRenameModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Nothing] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val oldTableName = alterTableRenameModel.oldTableIdentifier.table.toLowerCase
    val newTableName = alterTableRenameModel.newTableIdentifier.table.toLowerCase
    val oldDatabaseName = alterTableRenameModel.oldTableIdentifier.database
      .getOrElse(sparkSession.catalog.currentDatabase)
    val oldTableIdentifier = TableIdentifier(oldTableName, Some(oldDatabaseName))
    val newTableIdentifier = TableIdentifier(newTableName, Some(oldDatabaseName))
    setAuditTable(oldDatabaseName, oldTableIdentifier.table)
    setAuditInfo(Map("newName" -> alterTableRenameModel.newTableIdentifier.table))
    val newDatabaseName = newTableIdentifier.database
      .getOrElse(sparkSession.catalog.currentDatabase)
    if (!oldDatabaseName.equalsIgnoreCase(newDatabaseName)) {
      throw new MalformedCarbonCommandException("Database name should be same for both tables")
    }
    val tableExists = sparkSession.catalog.tableExists(oldDatabaseName, newTableIdentifier.table)
    if (tableExists) {
      throw new MalformedCarbonCommandException(s"Table with name $newTableIdentifier " +
                                                s"already exists")
    }
    LOGGER.info(s"Rename table request has been received for $oldDatabaseName.$oldTableName")
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val relation: CarbonRelation =
      metastore.lookupRelation(oldTableIdentifier.database, oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      throwMetadataException(oldDatabaseName, oldTableName, "Table does not exist")
    }

    var oldCarbonTable: CarbonTable = null
    oldCarbonTable = metastore.lookupRelation(Some(oldDatabaseName), oldTableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    if (!oldCarbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (!oldCarbonTable.canAllow(oldCarbonTable, TableOperation.ALTER_RENAME)) {
      throw new MalformedCarbonCommandException("alter rename is not supported for index datamap")
    }
    // if table have created MV, not support table rename
    if (oldCarbonTable.hasMVCreated || oldCarbonTable.isChildTableForMV) {
      throw new MalformedCarbonCommandException(
        "alter rename is not supported for datamap table or for tables which have child datamap")
    }

    var timeStamp = 0L
    var carbonTable: CarbonTable = null
    var hiveRenameSuccess = false
    // lock file path to release locks after operation
    var carbonTableLockFilePath: String = null
    try {
      carbonTable = metastore.lookupRelation(Some(oldDatabaseName), oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable
      carbonTableLockFilePath = carbonTable.getTablePath
      // if any load is in progress for table, do not allow rename table
      if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
        throw new ConcurrentOperationException(carbonTable, "loading", "alter table rename")
      }
      // get the old table all data map schema
      val dataMapSchemaList: util.List[DataMapSchema] = new util.ArrayList[DataMapSchema]()
      val indexSchemas = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
      if (!indexSchemas.isEmpty) {
        dataMapSchemaList.addAll(indexSchemas)
      }
      // invalid data map for the old table, see CARBON-1690
      val oldAbsoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      DataMapStoreManager.getInstance().clearDataMaps(oldAbsoluteTableIdentifier)
      // get the latest carbon table and check for column existence
      val operationContext = new OperationContext
      // TODO: Pass new Table Path in pre-event.
      val alterTableRenamePreEvent: AlterTableRenamePreEvent = AlterTableRenamePreEvent(
        carbonTable,
        alterTableRenameModel,
        "",
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableRenamePreEvent, operationContext)
      val tableInfo: org.apache.carbondata.format.TableInfo =
        metastore.getThriftTableInfo(carbonTable)
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setTableName(newTableName)
      timeStamp = System.currentTimeMillis()
      schemaEvolutionEntry.setTime_stamp(timeStamp)
      val newCarbonTableIdentifier = new CarbonTableIdentifier(oldDatabaseName,
        newTableName, carbonTable.getCarbonTableIdentifier.getTableId)
      metastore.removeTableFromMetadata(oldDatabaseName, oldTableName)
      var partitions: Seq[CatalogTablePartition] = Seq.empty
      if (carbonTable.isHivePartitionTable) {
        partitions =
          sparkSession.sessionState.catalog.listPartitions(oldTableIdentifier)
      }
      sparkSession.catalog.refreshTable(oldTableIdentifier.quotedString)
      CarbonSessionCatalogUtil.alterTableRename(
        oldTableIdentifier,
        newTableIdentifier,
        oldAbsoluteTableIdentifier.getTablePath,
        sparkSession,
        carbonTable.isExternalTable)
      hiveRenameSuccess = true

      metastore.updateTableSchemaForAlter(
        newCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        tableInfo,
        schemaEvolutionEntry,
        carbonTable.getTablePath)(sparkSession)

      // Update the storage location with datamap schema
      if (!dataMapSchemaList.isEmpty) {
        DataMapStoreManager.getInstance().
          updateDataMapSchema(dataMapSchemaList, newTableName)
      }
      val alterTableRenamePostEvent: AlterTableRenamePostEvent = AlterTableRenamePostEvent(
        carbonTable,
        alterTableRenameModel,
        oldAbsoluteTableIdentifier.getTablePath,
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableRenamePostEvent, operationContext)

      sparkSession.catalog.refreshTable(newTableIdentifier.quotedString)
      LOGGER.info(s"Table $oldTableName has been successfully renamed to $newTableName")
    } catch {
      case e: ConcurrentOperationException =>
        throw e
      case e: Exception =>
        if (hiveRenameSuccess) {
          CarbonSessionCatalogUtil.alterTableRename(
            newTableIdentifier,
            oldTableIdentifier,
            carbonTable.getAbsoluteTableIdentifier.getTablePath,
            sparkSession,
            carbonTable.isExternalTable)
        }
        if (carbonTable != null) {
          AlterTableUtil.revertRenameTableChanges(
            newTableName,
            carbonTable,
            timeStamp)(
            sparkSession)
        }
        throwMetadataException(oldDatabaseName, oldTableName,
          opName + " operation failed: " + e.getMessage)
    }
    Seq.empty
  }

  override protected def opName: String = "ALTER TABLE RENAME TABLE"
}
