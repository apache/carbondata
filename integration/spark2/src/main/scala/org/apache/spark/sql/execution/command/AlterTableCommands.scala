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

package org.apache.spark.sql.execution.command

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.hive.{CarbonRelation, HiveExternalCatalog}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionary
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.format.{ColumnSchema, SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataTypeConverterUtil}

private[sql] case class AlterTableAddColumns(
    alterTableAddColumnsModel: AlterTableAddColumnsModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableAddColumnsModel.tableName
    val dbName = alterTableAddColumnsModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table add columns request has been received for $dbName.$tableName")
    val carbonLock = AlterTableUtil
      .validateTableAndAcquireLock(dbName, tableName, LOGGER)(sparkSession)
    try {
      // get the latest carbon table and check for column existence
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val thriftTableInfo: TableInfo = CarbonEnv.get.carbonMetastore
        .readSchemaFile(tableMetadataFile)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(thriftTableInfo,
          dbName,
          tableName,
          carbonTable.getStorePath)
      val newCols = new AlterTableProcessor(alterTableAddColumnsModel,
        dbName,
        wrapperTableInfo,
        carbonTablePath,
        carbonTable.getCarbonTableIdentifier,
        carbonTable.getStorePath).process
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
      schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis)
      schemaEvolutionEntry.setAdded(newCols.toList.asJava)
      val thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
      thriftTable.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry),
          thriftTable)(sparkSession,
          sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog])
      LOGGER.info(s"Alter table for add columns is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for add columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error("Alter table add columns failed : " + e.getMessage)
        throw e
    } finally {
      // release lock after command execution completion
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          LOGGER.info("Alter table add columns lock released successfully")
        } else {
          LOGGER.error("Unable to release lock during alter table add columns operation")
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class AlterTableRenameTable(alterTableRenameModel: AlterTableRenameModel)
  extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
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
    val relation: CarbonRelation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(oldTableIdentifier.database, oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Rename table request has failed. " +
                   s"Table $oldDatabaseName.$oldTableName does not exist")
      sys.error(s"Table $oldDatabaseName.$oldTableName does not exist")
    }
    val carbonTable = relation.tableMeta.carbonTable
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK)
    if (carbonLock.lockWithRetries()) {
      LOGGER.info("Successfully able to get the table metadata file lock")
    } else {
      sys.error("Table is locked for updation. Please try after some time")
    }
    try {
      // get the latest carbon table and check for column existence
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val tableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.get.carbonMetastore
        .readSchemaFile(tableMetadataFile)
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setTableName(newTableName)
      renameBadRecords(oldTableName, newTableName, oldDatabaseName)
      val fileType = FileFactory.getFileType(tableMetadataFile)
      if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
        val rename = FileFactory.getCarbonFile(carbonTablePath.getPath, fileType)
          .renameForce(carbonTablePath.getParent.toString + CarbonCommonConstants.FILE_SEPARATOR +
                       newTableName)
        if (!rename) {
          sys.error(s"Folder rename failed for table $oldDatabaseName.$oldTableName")
          renameBadRecords(newTableName, oldTableName, oldDatabaseName)
        }
      }
      val newTableIdentifier = new CarbonTableIdentifier(oldDatabaseName,
        newTableName,
        carbonTable.getCarbonTableIdentifier.getTableId)
      val newTablePath = CarbonEnv.get.carbonMetastore.updateTableSchema(newTableIdentifier,
        tableInfo,
        schemaEvolutionEntry,
        carbonTable.getStorePath)(sparkSession)
      CarbonEnv.get.carbonMetastore.removeTableFromMetadata(oldDatabaseName, oldTableName)
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .runSqlHive(
          s"ALTER TABLE $oldDatabaseName.$oldTableName RENAME TO $oldDatabaseName.$newTableName")
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
        .runSqlHive(
          s"ALTER TABLE $oldDatabaseName.$newTableName SET SERDEPROPERTIES" +
          s"('tableName'='$newTableName', " +
          s"'dbName'='$oldDatabaseName', 'tablePath'='$newTablePath')")
      LOGGER.audit(s"Table $oldTableName has been successfully renamed to $newTableName")
      LOGGER.info(s"Table $oldTableName has been successfully renamed to $newTableName")
    } catch {
      case e: Exception => LOGGER.error("Rename table failed: " + e.getMessage)
        throw e
    } finally {
      // release lock after command execution completion
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          LOGGER.info("Lock released successfully")
        } else {
          LOGGER.error("Unable to release lock during rename table")
        }
      }
    }
    Seq.empty
  }

  private def renameBadRecords(oldTableName: String,
      newTableName: String,
      dataBaseName: String) = {
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

private[sql] case class AlterTableDropColumns(
    alterTableDropColumnModel: AlterTableDropColumnModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableDropColumnModel.tableName
    val dbName = alterTableDropColumnModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table drop columns request has been received for $dbName.$tableName")
    val carbonLock = AlterTableUtil
      .validateTableAndAcquireLock(dbName, tableName, LOGGER)(sparkSession)
    try {
      // get the latest carbon table and check for column existence
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
      // check each column existence in the table
      val tableColumns = carbonTable.getCreateOrderColumn(tableName).asScala
      var dictionaryColumns = ListBuffer[CarbonColumn]()
      var keyColumnCountToBeDeleted = 0
      // TODO: if deleted column list includes shared dictionary/bucketted column throw an error
      alterTableDropColumnModel.columns.foreach { column =>
        var columnExist = false
        tableColumns.foreach { tableColumn =>
          // column should not be already deleted and should exist in the table
          if (!tableColumn.isInvisible && column.equalsIgnoreCase(tableColumn.getColName)) {
            if (tableColumn.isDimesion) {
              keyColumnCountToBeDeleted += 1
              if (tableColumn.hasEncoding(Encoding.DICTIONARY)) {
                dictionaryColumns += tableColumn
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
      var totalKeyColumnInSchema = 0
      tableColumns.foreach { tableColumn =>
        // column should not be already deleted and should exist in the table
        if (!tableColumn.isInvisible && tableColumn.isDimesion) {
          totalKeyColumnInSchema += 1
        }
      }
      if (keyColumnCountToBeDeleted >= totalKeyColumnInSchema) {
        sys.error(s"Alter drop operation failed. AtLeast one key column should exist after drop.")
      }
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val tableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.get.carbonMetastore
        .readSchemaFile(tableMetadataFile)
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
      tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setRemoved(deletedColumnSchema.toList.asJava)
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaEvolutionEntry,
          tableInfo)(sparkSession,
          sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog])
      // TODO: 1. add check for deletion of index tables
      // delete dictionary files for dictionary column and clear dictionary cache from memory
      ManageDictionary.deleteDictionaryFileAndCache(dictionaryColumns.toList.asJava, carbonTable)
      LOGGER.info(s"Alter table for drop columns is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for drop columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error("Alter table drop columns failed : " + e.getMessage)
        throw e
    } finally {
      // release lock after command execution completion
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          LOGGER.info("Alter table drop columns lock released successfully")
        } else {
          LOGGER.error("Unable to release lock during alter table drop columns operation")
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class AlterTableDataTypeChange(
    alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableDataTypeChangeModel.tableName
    val dbName = alterTableDataTypeChangeModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table change data type request has been received for $dbName.$tableName")
    val carbonLock = AlterTableUtil
      .validateTableAndAcquireLock(dbName, tableName, LOGGER)(sparkSession)
    try {
      // get the latest carbon table and check for column existence
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
      val columnName = alterTableDataTypeChangeModel.columnName
      var carbonColumnToBeModified: CarbonColumn = null
      val carbonColumns = carbonTable.getCreateOrderColumn(tableName).asScala.filter(!_.isInvisible)

      if (!carbonColumns.exists(_.getColName.equalsIgnoreCase(columnName))) {
        LOGGER.audit(s"Alter table change data type request has failed. " +
                     s"Column $columnName does not exist")
        sys.error(s"Column does not exist: $columnName")
      }
      val carbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(columnName))
      if (carbonColumn.size == 1) {
        CarbonScalaUtil
          .validateColumnDataType(alterTableDataTypeChangeModel.dataTypeInfo, carbonColumn(0))
      } else {
        LOGGER.audit(s"Alter table change data type request has failed. " +
                     s"Column $columnName is invalid")
        sys.error(s"Invalid Column: $columnName")
      }
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val tableInfo: TableInfo = CarbonEnv.get.carbonMetastore
        .readSchemaFile(tableMetadataFile)
      // maintain the added column for schema evolution history
      var addColumnSchema: ColumnSchema = null
      var deletedColumnSchema: ColumnSchema = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)
      columnSchemaList.foreach { columnSchema =>
        if (columnSchema.column_name.equalsIgnoreCase(columnName)) {
          deletedColumnSchema = columnSchema.deepCopy
          columnSchema.setData_type(DataTypeConverterUtil
            .convertToThriftDataType(alterTableDataTypeChangeModel.dataTypeInfo.dataType))
          columnSchema.setPrecision(alterTableDataTypeChangeModel.dataTypeInfo.precision)
          columnSchema.setScale(alterTableDataTypeChangeModel.dataTypeInfo.scale)
          addColumnSchema = columnSchema
        }
      }
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setAdded(List(addColumnSchema).asJava)
      schemaEvolutionEntry.setRemoved(List(deletedColumnSchema).asJava)
      tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaEvolutionEntry,
          tableInfo)(sparkSession,
          sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog])
      LOGGER.info(s"Alter table for data type change is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for data type change is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error("Alter table change datatype failed : " + e.getMessage)
        throw e
    } finally {
      // release lock after command execution completion
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          LOGGER.info("Alter table change data type lock released successfully")
        } else {
          LOGGER.error("Unable to release lock during alter table change data type operation")
        }
      }
    }
    Seq.empty
  }
}
