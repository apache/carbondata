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
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionState}
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.format.{ColumnSchema, SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.rdd.{AlterTableAddColumnRDD, AlterTableDropColumnRDD}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataTypeConverterUtil}

private[sql] case class AlterTableAddColumns(
    alterTableAddColumnsModel: AlterTableAddColumnsModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableAddColumnsModel.tableName
    val dbName = alterTableAddColumnsModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table add columns request has been received for $dbName.$tableName")
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
      carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        .tableMeta.carbonTable
      // get the latest carbon table and check for column existence
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val thriftTableInfo: TableInfo = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .readSchemaFile(tableMetadataFile)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(thriftTableInfo,
          dbName,
          tableName,
          carbonTable.getStorePath)
      newCols = new AlterTableColumnSchemaGenerator(alterTableAddColumnsModel,
        dbName,
        wrapperTableInfo,
        carbonTablePath,
        carbonTable.getCarbonTableIdentifier,
        carbonTable.getStorePath, sparkSession.sparkContext).process
      // generate dictionary files for the newly added columns
      new AlterTableAddColumnRDD(sparkSession.sparkContext,
        newCols,
        carbonTable.getCarbonTableIdentifier,
        carbonTable.getStorePath).collect()
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
      schemaEvolutionEntry.setTimeStamp(timeStamp)
      schemaEvolutionEntry.setAdded(newCols.toList.asJava)
      val thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
      val sessionState = sparkSession.sessionState.asInstanceOf[CarbonSessionState]
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry),
          thriftTable)(sparkSession, sessionState)
      val newFields = alterTableAddColumnsModel.dimCols ++ alterTableAddColumnsModel.msrCols
      val useCompatibleSchema = sparkSession.sparkContext.conf
        .getBoolean(CarbonCommonConstants.SPARK_SCHEMA_HIVE_COMPATIBILITY_ENABLE, false)
      if (useCompatibleSchema) {
        sessionState.metadataHive.runSqlHive(s"ALTER TABLE $dbName.$tableName " +
          s"ADD COLUMNS(${newFields.sortBy(_.schemaOrdinal).map(f => f.rawSchema).mkString(",")})")
      }
      LOGGER.info(s"Alter table for add columns is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for add columns is successful for table $dbName.$tableName")
    } catch {
      case e: Exception => LOGGER
        .error("Alter table add columns failed :" + e.getMessage)
        if (newCols.nonEmpty) {
          LOGGER.info("Cleaning up the dictionary files as alter table add operation failed")
          new AlterTableDropColumnRDD(sparkSession.sparkContext,
            newCols,
            carbonTable.getCarbonTableIdentifier,
            carbonTable.getStorePath).collect()
          AlterTableUtil.revertAddColumnChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        sys.error(s"Alter table add operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
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
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(oldTableIdentifier.database, oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Rename table request has failed. " +
                   s"Table $oldDatabaseName.$oldTableName does not exist")
      sys.error(s"Table $oldDatabaseName.$oldTableName does not exist")
    }
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
      LockUsage.COMPACTION_LOCK,
      LockUsage.DELETE_SEGMENT_LOCK,
      LockUsage.CLEAN_FILES_LOCK,
      LockUsage.DROP_TABLE_LOCK)
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(oldDatabaseName, oldTableName, locksToBeAcquired)(
            sparkSession)
      carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Some(oldDatabaseName), oldTableName)(sparkSession)
        .asInstanceOf[CarbonRelation].tableMeta.carbonTable
      // get the latest carbon table and check for column existence
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val tableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.getInstance(sparkSession)
        .carbonMetastore.readSchemaFile(tableMetadataFile)
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setTableName(newTableName)
      timeStamp = System.currentTimeMillis()
      schemaEvolutionEntry.setTime_stamp(timeStamp)
      renameBadRecords(oldTableName, newTableName, oldDatabaseName)
      val fileType = FileFactory.getFileType(tableMetadataFile)
      if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
        val rename = FileFactory.getCarbonFile(carbonTablePath.getPath, fileType)
          .renameForce(carbonTablePath.getParent.toString + CarbonCommonConstants.FILE_SEPARATOR +
                       newTableName)
        if (!rename) {
          renameBadRecords(newTableName, oldTableName, oldDatabaseName)
          sys.error(s"Folder rename failed for table $oldDatabaseName.$oldTableName")
        }
      }
      val newTableIdentifier = new CarbonTableIdentifier(oldDatabaseName,
        newTableName,
        carbonTable.getCarbonTableIdentifier.getTableId)
      val newTablePath = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .updateTableSchema(newTableIdentifier,
          tableInfo,
          schemaEvolutionEntry,
          carbonTable.getStorePath)(sparkSession)
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .removeTableFromMetadata(oldDatabaseName, oldTableName)
      val sessionState = sparkSession.sessionState.asInstanceOf[CarbonSessionState]
      sessionState.metadataHive
        .runSqlHive(
          s"ALTER TABLE $oldDatabaseName.$oldTableName RENAME TO $oldDatabaseName.$newTableName")
      sessionState.metadataHive
        .runSqlHive(
          s"ALTER TABLE $oldDatabaseName.$newTableName SET SERDEPROPERTIES" +
          s"('tableName'='$newTableName', " +
          s"'dbName'='$oldDatabaseName', 'tablePath'='$newTablePath')")
      LOGGER.audit(s"Table $oldTableName has been successfully renamed to $newTableName")
      LOGGER.info(s"Table $oldTableName has been successfully renamed to $newTableName")
    } catch {
      case e: Exception => LOGGER
        .error("Rename table failed: " + e.getMessage)
        if (carbonTable != null) {
          AlterTableUtil
            .revertRenameTableChanges(oldTableIdentifier,
              newTableName,
              carbonTable.getStorePath,
              carbonTable.getCarbonTableIdentifier.getTableId,
              timeStamp)(
              sparkSession)
          renameBadRecords(newTableName, oldTableName, oldDatabaseName)
        }
        sys.error(s"Alter table rename table operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
      // case specific to rename table as after table rename old table path will not be found
      if (carbonTable != null) {
        AlterTableUtil
          .releaseLocksManually(locks,
            locksToBeAcquired,
            oldDatabaseName,
            newTableName,
            carbonTable.getStorePath)
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
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    // get the latest carbon table and check for column existence
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        .tableMeta.carbonTable
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
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val tableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.getInstance(sparkSession)
        .carbonMetastore.readSchemaFile(tableMetadataFile)
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
      val sessionState = sparkSession.sessionState.asInstanceOf[CarbonSessionState]
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaEvolutionEntry,
          tableInfo)(sparkSession, sessionState)
      // TODO: 1. add check for deletion of index tables
      // delete dictionary files for dictionary column and clear dictionary cache from memory
      new AlterTableDropColumnRDD(sparkSession.sparkContext,
        dictionaryColumns,
        carbonTable.getCarbonTableIdentifier,
        carbonTable.getStorePath).collect()
      val useCompatibleSchema = sparkSession.sparkContext.conf
        .getBoolean(CarbonCommonConstants.SPARK_SCHEMA_HIVE_COMPATIBILITY_ENABLE, false)
      if (useCompatibleSchema) {
        deletedColumnSchema.foreach { col =>
          sessionState.metadataHive.runSqlHive(s"ALTER TABLE $dbName.$tableName " +
            s"DROP COLUMN ${col.column_name}")
        }
      }
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

private[sql] case class AlterTableDataTypeChange(
    alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableDataTypeChangeModel.tableName
    val dbName = alterTableDataTypeChangeModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table change data type request has been received for $dbName.$tableName")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    // get the latest carbon table and check for column existence
    var carbonTable: CarbonTable = null
    var timeStamp = 0L
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
        .tableMeta.carbonTable
      val columnName = alterTableDataTypeChangeModel.columnName
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
      val tableInfo: TableInfo = CarbonEnv.getInstance(sparkSession).carbonMetastore
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
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new SchemaEvolutionEntry(timeStamp)
      schemaEvolutionEntry.setAdded(List(addColumnSchema).asJava)
      schemaEvolutionEntry.setRemoved(List(deletedColumnSchema).asJava)
      tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      val sessionState = sparkSession.sessionState.asInstanceOf[CarbonSessionState]
      AlterTableUtil
        .updateSchemaInfo(carbonTable,
          schemaEvolutionEntry,
          tableInfo)(sparkSession, sessionState)
      val useCompatibleSchema = sparkSession.sparkContext.conf
        .getBoolean(CarbonCommonConstants.SPARK_SCHEMA_HIVE_COMPATIBILITY_ENABLE, false)
      if (useCompatibleSchema) {
        val dataTypeInfo = alterTableDataTypeChangeModel.dataTypeInfo
        val colSchema = if (dataTypeInfo.dataType == "decimal") {
          s"decimal(${dataTypeInfo.precision},${dataTypeInfo.scale}"
        } else {
          dataTypeInfo.dataType
        }
        sessionState.metadataHive.runSqlHive(s"ALTER TABLE $dbName.$tableName " +
          s"ALTER COLUMN $columnName $colSchema")
      }
      LOGGER.info(s"Alter table for data type change is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for data type change is successful for table $dbName.$tableName")
    } catch {
      case e: Exception => LOGGER
        .error("Alter table change datatype failed : " + e.getMessage)
        if (carbonTable != null) {
          AlterTableUtil.revertDataTypeChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        sys.error(s"Alter table data type change operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }
}
