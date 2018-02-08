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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{AlterTableRenameModel, MetadataCommand}
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionCatalog}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableRenamePostEvent, AlterTableRenamePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.SchemaEvolutionEntry
import org.apache.carbondata.spark.exception.{ConcurrentOperationException, MalformedCarbonCommandException}

private[sql] case class CarbonAlterTableRenameCommand(
    alterTableRenameModel: AlterTableRenameModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Nothing] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val oldTableIdentifier = alterTableRenameModel.oldTableIdentifier
    val newTableIdentifier = alterTableRenameModel.newTableIdentifier
    val oldDatabaseName = oldTableIdentifier.database
      .getOrElse(sparkSession.catalog.currentDatabase)
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
    val oldTableName = oldTableIdentifier.table.toLowerCase
    val newTableName = newTableIdentifier.table.toLowerCase
    LOGGER.audit(s"Rename table request has been received for $oldDatabaseName.$oldTableName")
    LOGGER.info(s"Rename table request has been received for $oldDatabaseName.$oldTableName")
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val relation: CarbonRelation =
      metastore.lookupRelation(oldTableIdentifier.database, oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Rename table request has failed. " +
                   s"Table $oldDatabaseName.$oldTableName does not exist")
      throwMetadataException(oldDatabaseName, oldTableName, "Table does not exist")
    }
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
      LockUsage.COMPACTION_LOCK,
      LockUsage.DELETE_SEGMENT_LOCK,
      LockUsage.CLEAN_FILES_LOCK,
      LockUsage.DROP_TABLE_LOCK)
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    var carbonTable: CarbonTable = null
    // lock file path to release locks after operation
    var carbonTableLockFilePath: String = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(oldDatabaseName, oldTableName, locksToBeAcquired)(
          sparkSession)
      carbonTable = metastore.lookupRelation(Some(oldDatabaseName), oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable
      carbonTableLockFilePath = carbonTable.getTablePath
      // if any load is in progress for table, do not allow rename table
      if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
        throw new ConcurrentOperationException(carbonTable, "loading", "alter table rename")
      }
      // invalid data map for the old table, see CARBON-1690
      val oldTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      DataMapStoreManager.getInstance().clearDataMaps(oldTableIdentifier)
      // get the latest carbon table and check for column existence
      val tableMetadataFile = oldTableIdentifier.getTablePath
      val operationContext = new OperationContext
      // TODO: Pass new Table Path in pre-event.
      val alterTableRenamePreEvent: AlterTableRenamePreEvent = AlterTableRenamePreEvent(
        carbonTable,
        alterTableRenameModel,
        "",
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableRenamePreEvent, operationContext)
      val tableInfo: org.apache.carbondata.format.TableInfo =
        metastore.getThriftTableInfo(carbonTable)(sparkSession)
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setTableName(newTableName)
      timeStamp = System.currentTimeMillis()
      schemaEvolutionEntry.setTime_stamp(timeStamp)
      renameBadRecords(oldTableName, newTableName, oldDatabaseName)
      val fileType = FileFactory.getFileType(tableMetadataFile)
      val newTableIdentifier = new CarbonTableIdentifier(oldDatabaseName,
        newTableName, carbonTable.getCarbonTableIdentifier.getTableId)
      var newTablePath = CarbonTablePath.getNewTablePath(
        oldTableIdentifier.getTablePath, newTableIdentifier.getTableName)
      metastore.removeTableFromMetadata(oldDatabaseName, oldTableName)
      val hiveClient = sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog]
        .getClient()
      sparkSession.catalog.refreshTable(TableIdentifier(oldTableName,
        Some(oldDatabaseName)).quotedString)
      hiveClient.runSqlHive(
          s"ALTER TABLE $oldDatabaseName.$oldTableName RENAME TO $oldDatabaseName.$newTableName")
      hiveClient.runSqlHive(
          s"ALTER TABLE $oldDatabaseName.$newTableName SET SERDEPROPERTIES" +
          s"('tableName'='$newTableName', " +
          s"'dbName'='$oldDatabaseName', 'tablePath'='$newTablePath')")
      // changed the rename order to deal with situation when carbon table and hive table
      // will point to the same tablePath
      if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
        val rename = FileFactory.getCarbonFile(oldTableIdentifier.getTablePath, fileType)
          .renameForce(
            CarbonTablePath.getNewTablePath(oldTableIdentifier.getTablePath, newTableName))
        if (!rename) {
          renameBadRecords(newTableName, oldTableName, oldDatabaseName)
          sys.error(s"Folder rename failed for table $oldDatabaseName.$oldTableName")
        }
      }
      newTablePath = metastore.updateTableSchemaForAlter(
        newTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        tableInfo,
        schemaEvolutionEntry,
        carbonTable.getTablePath)(sparkSession)

      val alterTableRenamePostEvent: AlterTableRenamePostEvent = AlterTableRenamePostEvent(
        carbonTable,
        alterTableRenameModel,
        newTablePath,
        sparkSession)
      OperationListenerBus.getInstance().fireEvent(alterTableRenamePostEvent, operationContext)

      sparkSession.catalog.refreshTable(TableIdentifier(newTableName,
        Some(oldDatabaseName)).quotedString)
      carbonTableLockFilePath = newTablePath
      LOGGER.audit(s"Table $oldTableName has been successfully renamed to $newTableName")
      LOGGER.info(s"Table $oldTableName has been successfully renamed to $newTableName")
    } catch {
      case e: ConcurrentOperationException =>
        throw e
      case e: Exception =>
        LOGGER.error(e, "Rename table failed: " + e.getMessage)
        if (carbonTable != null) {
          AlterTableUtil.revertRenameTableChanges(
            newTableName,
            carbonTable,
            timeStamp)(
            sparkSession)
          renameBadRecords(newTableName, oldTableName, oldDatabaseName)
        }
        throwMetadataException(oldDatabaseName, oldTableName,
          s"Alter table rename table operation failed: ${e.getMessage}")
    } finally {
      // case specific to rename table as after table rename old table path will not be found
      if (carbonTable != null) {
        AlterTableUtil
          .releaseLocksManually(locks,
            locksToBeAcquired,
            oldDatabaseName,
            newTableName,
            carbonTableLockFilePath)
      }
    }
    Seq.empty
  }

  private def renameBadRecords(
      oldTableName: String,
      newTableName: String,
      dataBaseName: String): Unit = {
    val oldPath = CarbonUtil
      .getBadLogPath(dataBaseName + CarbonCommonConstants.FILE_SEPARATOR + oldTableName)
    val newPath = CarbonUtil
      .getBadLogPath(dataBaseName + CarbonCommonConstants.FILE_SEPARATOR + newTableName)
    val fileType = FileFactory.getFileType(oldPath)
    if (FileFactory.isFileExist(oldPath, fileType)) {
      val renameSuccess = FileFactory.getCarbonFile(oldPath, fileType)
        .renameForce(newPath)
      if (!renameSuccess) {
        sys.error(s"BadRecords Folder Rename Failed for table $dataBaseName.$oldTableName")
      }
    }
  }

}
