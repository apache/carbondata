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

import java.io.File
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{CarbonMetastore, CarbonRelation, HiveExternalCatalog}
import org.apache.spark.util.FileUtils
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.format.SchemaEvolutionEntry
import org.apache.carbondata.processing.constants.TableOptionConstant
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, DictionaryLoadModel}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CarbonSparkUtil, CommonUtil, DataTypeConverterUtil, GlobalDictionaryUtil}

object Checker {
  def validateTableExists(
      dbName: Option[String],
      tableName: String,
      session: SparkSession): Unit = {
    val identifier = TableIdentifier(tableName, dbName)
    if (!CarbonEnv.get.carbonMetastore.tableExists(identifier)(session)) {
      val err = s"table $dbName.$tableName not found"
      LogServiceFactory.getLogService(this.getClass.getName).error(err)
      throw new IllegalArgumentException(err)
    }
  }
}

/**
 * Command for the compaction in alter table command
 *
 * @param alterTableModel
 */
case class AlterTableCompaction(alterTableModel: AlterTableModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableModel.tableName.toLowerCase
    val databaseName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    if (null == CarbonMetadata.getInstance.getCarbonTable(databaseName + "_" + tableName)) {
      LOGGER.error(s"alter table failed. table not found: $databaseName.$tableName")
      sys.error(s"alter table failed. table not found: $databaseName.$tableName")
    }

    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(databaseName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()


    val table = relation.tableMeta.carbonTable
    carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    carbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
    carbonLoadModel.setStorePath(relation.tableMeta.storePath)

    val kettleHomePath = CarbonScalaUtil.getKettleHome(sparkSession.sqlContext)

    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir")
      )
    storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
    try {
      CarbonDataRDDFactory
        .alterTableForCompaction(sparkSession.sqlContext,
          alterTableModel,
          carbonLoadModel,
          relation.tableMeta.storePath,
          kettleHomePath,
          storeLocation
        )
    } catch {
      case e: Exception =>
        if (null != e.getMessage) {
          sys.error(s"Compaction failed. Please check logs for more info. ${ e.getMessage }")
        } else {
          sys.error("Exception in compaction. Please check logs for more info.")
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
    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Alter table change data type request has failed. " +
                   s"Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    // acquire the lock first
    val table = relation.tableMeta.carbonTable
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(table.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK)
    try {
      // get the latest carbon table and check for column existence
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
      val columnName = alterTableDataTypeChangeModel.columnName
      var carbonColumnToBeModified: CarbonColumn = null
      val carbonColumns = carbonTable.getCreateOrderColumn(tableName).asScala
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val tableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.get.carbonMetastore
        .readSchemaFile(tableMetadataFile)
      // maintain the added column for schema evolution history
      var addColumnSchema: org.apache.carbondata.format.ColumnSchema = null
      var deletedColumnSchema: org.apache.carbondata.format.ColumnSchema = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala
      columnSchemaList.foreach { columnSchema =>
        if (columnSchema.column_name.equalsIgnoreCase(columnName)) {
          deletedColumnSchema = CarbonScalaUtil.createColumnSchemaCopyObject(columnSchema)
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
      CarbonEnv.get.carbonMetastore
        .updateTableSchema(carbonTable.getCarbonTableIdentifier,
          tableInfo,
          schemaEvolutionEntry,
          carbonTable.getStorePath)(sparkSession)

      val tableIdentifier = TableIdentifier(tableName, Some(dbName))
      val schema = CarbonEnv.get.carbonMetastore
        .lookupRelation(tableIdentifier)(sparkSession).schema.json
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client.runSqlHive(
        s"ALTER TABLE $dbName.$tableName SET TBLPROPERTIES('spark.sql.sources.schema'='$schema')")
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
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

private[sql] case class AlterTableAddColumns(
    alterTableAddColumnsModel: AlterTableAddColumnsModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableAddColumnsModel.tableName
    val dbName = alterTableAddColumnsModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table add columns request has been received for $dbName.$tableName")
    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Alter table add columns request has failed. " +
                   s"Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    // acquire the lock first
    val table = relation.tableMeta.carbonTable
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(table.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK)
    try {
      // get the latest carbon table and check for column existence
      val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
        carbonTable.getCarbonTableIdentifier)
      val tableMetadataFile = carbonTablePath.getSchemaFilePath
      val thriftTableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.get.carbonMetastore
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
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata
      .schema.SchemaEvolutionEntry()
      schemaEvolutionEntry.setTimeStamp(System.currentTimeMillis)
      schemaEvolutionEntry.setAdded(newCols.toList.asJava)
      val thriftTable = schemaConverter
        .fromWrapperToExternalTableInfo(wrapperTableInfo, dbName, tableName)
      thriftTable.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      CarbonEnv.get.carbonMetastore
        .updateTableSchema(carbonTable.getCarbonTableIdentifier,
          thriftTable,
          schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry),
          carbonTable.getStorePath)(sparkSession)
      val tableIdentifier = TableIdentifier(tableName, Some(dbName))
      val schema = CarbonEnv.get.carbonMetastore
        .lookupRelation(tableIdentifier)(sparkSession).schema.json
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client.runSqlHive(
        s"ALTER TABLE $dbName.$tableName SET TBLPROPERTIES('spark.sql.sources.schema'='$schema')")
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
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
      val fileType = FileFactory.getFileType(tableMetadataFile)
      if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
        val rename = FileFactory.getCarbonFile(carbonTablePath.getPath, fileType)
          .renameForce(carbonTablePath.getParent.toString + CarbonCommonConstants.FILE_SEPARATOR +
                       newTableName)
        if (!rename) {
          sys.error(s"Rename failed for table $oldTableName")
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
          s"ALTER TABLE $oldDatabaseName.$oldTableName RENAME TO $newTableName")
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
}

private[sql] case class AlterTableDropColumns(
    alterTableDropColumnModel: AlterTableDropColumnModel) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    val tableName = alterTableDropColumnModel.tableName
    val dbName = alterTableDropColumnModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table drop columns request has been received for $dbName.$tableName")
    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Alter table drop columns request has failed. " +
                   s"Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    // acquire the lock first
    val table = relation.tableMeta.carbonTable
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(table.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK)
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
            deletedColumnSchema += CarbonScalaUtil.createColumnSchemaCopyObject(columnSchema)
            columnSchema.invisible = true
          }
        }
      }
      // add deleted columns to schema evolution history and update the schema
      tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setRemoved(deletedColumnSchema.toList.asJava)
      CarbonEnv.get.carbonMetastore
        .updateTableSchema(carbonTable.getCarbonTableIdentifier,
          tableInfo,
          schemaEvolutionEntry,
          carbonTable.getStorePath)(sparkSession)

      val tableIdentifier = TableIdentifier(tableName, Some(dbName))
      val schema = CarbonEnv.get.carbonMetastore
        .lookupRelation(tableIdentifier)(sparkSession).schema.json
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client.runSqlHive(
        s"ALTER TABLE $dbName.$tableName SET TBLPROPERTIES('spark.sql.sources.schema'='$schema')")
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
      // TODO: 1. add check for deletion of index tables
      // delete dictionary files for dictionary column and clear dictionary cache from memory
      CarbonUtil.deleteDictionaryFileAndCache(dictionaryColumns.toList.asJava, carbonTable)
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

case class CreateTable(cm: TableModel, createDSTable: Boolean = true) extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    CarbonEnv.init(sparkSession)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = getDB.getDatabaseName(cm.databaseNameOp, sparkSession)
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm)

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      sys.error("No Dimensions found. Table should have at least one dimesnion !")
    }

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tbName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
          s"Table [$tbName] already exists under database [$dbName]")
        sys.error(s"Table [$tbName] already exists under database [$dbName]")
      }
    } else {
      // Add Database to catalog and persist
      val catalog = CarbonEnv.get.carbonMetastore
      // Need to fill partitioner class when we support partition
      val tablePath = catalog.createTableFromThrift(tableInfo, dbName, tbName)(sparkSession)
      if (createDSTable) {
        try {
          val fields = new Array[Field](cm.dimCols.size + cm.msrCols.size)
          cm.dimCols.foreach(f => fields(f.schemaOrdinal) = f)
          cm.msrCols.foreach(f => fields(f.schemaOrdinal) = f)
          sparkSession.sql(
            s"""CREATE TABLE $dbName.$tbName
               |(${fields.map(f => f.rawSchema).mkString(",")})
               |USING org.apache.spark.sql.CarbonSource""".stripMargin +
            s""" OPTIONS (tableName "$tbName", dbName "$dbName", tablePath "$tablePath") """)
        } catch {
          case e: Exception =>
            val identifier: TableIdentifier = TableIdentifier(tbName, Some(dbName))
            // call the drop table to delete the created table.

            CarbonEnv.get.carbonMetastore
              .dropTable(catalog.storePath, identifier)(sparkSession)

            LOGGER.audit(s"Table creation with Database name [$dbName] " +
                         s"and Table name [$tbName] failed")
            throw e
        }
      }

      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
    }

    Seq.empty
  }

  def setV(ref: Any, name: String, value: Any): Unit = {
    ref.getClass.getFields.find(_.getName == name).get
      .set(ref, value.asInstanceOf[AnyRef])
  }
}

case class DeleteLoadsById(
    loadids: Seq[String],
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {

    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    CarbonStore.deleteLoadById(
      loadids,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName
    )
    Seq.empty

  }

}

case class DeleteLoadsByLoadDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    loadDate: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.TableModel.tableSchema")

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    CarbonStore.deleteLoadByDate(
      loadDate,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName
    )
    Seq.empty
  }

}

object LoadTable {

  def updateTableMetadata(carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      model: DictionaryLoadModel,
      noDictDimension: Array[CarbonDimension]): Unit = {

    val carbonTablePath = CarbonStorePath.getCarbonTablePath(model.hdfsLocation,
      model.table)
    val schemaFilePath = carbonTablePath.getSchemaFilePath

    // read TableInfo
    val tableInfo = CarbonMetastore.readSchemaFileToThriftTable(schemaFilePath)

    // modify TableInfo
    val columns = tableInfo.getFact_table.getTable_columns
    for (i <- 0 until columns.size) {
      if (noDictDimension.exists(x => columns.get(i).getColumn_id.equals(x.getColumnId))) {
        columns.get(i).encoders.remove(org.apache.carbondata.format.Encoding.DICTIONARY)
      }
    }

    // write TableInfo
    CarbonMetastore.writeThriftTableToSchemaFile(schemaFilePath, tableInfo)

    // update Metadata
    val catalog = CarbonEnv.get.carbonMetastore
    catalog.updateMetadataByThriftTable(schemaFilePath, tableInfo,
      model.table.getDatabaseName, model.table.getTableName, carbonLoadModel.getStorePath)

    // update CarbonDataLoadSchema
    val carbonTable = catalog.lookupRelation(Option(model.table.getDatabaseName),
      model.table.getTableName)(sqlContext.sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
  }

}

case class LoadTableByInsert(relation: CarbonDatasourceHadoopRelation, child: LogicalPlan) {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  def run(sparkSession: SparkSession): Seq[Row] = {
    val df = Dataset.ofRows(sparkSession, child)
    val header = relation.tableSchema.get.fields.map(_.name).mkString(",")
    val load = LoadTable(
      Some(relation.carbonRelation.databaseName),
      relation.carbonRelation.tableName,
      null,
      Seq(),
      scala.collection.immutable.Map("fileheader" -> header),
      isOverwriteExist = false,
      null,
      Some(df)).run(sparkSession)
    // updating relation metadata. This is in case of auto detect high cardinality
    relation.carbonRelation.metaData =
      CarbonSparkUtil.createSparkMeta(relation.carbonRelation.tableMeta.carbonTable)
    load
  }
}

case class LoadTable(
    databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: scala.collection.immutable.Map[String, String],
    isOverwriteExist: Boolean = false,
    var inputSqlString: String = null,
    dataFrame: Option[DataFrame] = None,
    updateModel: Option[UpdateTableModel] = None) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private def checkDefaultValue(value: String, default: String) = if (StringUtils.isEmpty(value)) {
    default
  } else {
    value
  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    if (dataFrame.isDefined && !updateModel.isDefined) {
      val rdd = dataFrame.get.rdd
      if (rdd.partitions == null || rdd.partitions.length == 0) {
        LOGGER.warn("DataLoading finished. No data was loaded.")
        return Seq.empty
      }
    }

    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    if (isOverwriteExist) {
      sys.error(s"Overwrite is not supported for carbon table with $dbName.$tableName")
    }
    if (null == CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)) {
      LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
      LOGGER.audit(s"Data loading failed. table not found: $dbName.$tableName")
      sys.error(s"Data loading failed. table not found: $dbName.$tableName")
    }

    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    CarbonProperties.getInstance().addProperty("zookeeper.enable.lock", "false")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(relation.tableMeta.carbonTable.getAbsoluteTableIdentifier
        .getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK
      )
    try {
      // take lock only in case of normal data load.
      if (!updateModel.isDefined) {
        if (carbonLock.lockWithRetries()) {
          LOGGER.info("Successfully able to get the table metadata file lock")
        } else {
          sys.error("Table is locked for updation. Please try after some time")
        }
      }

      val factPath = if (dataFrame.isDefined) {
        ""
      } else {
        FileUtils.getPaths(
          CarbonUtil.checkAndAppendHDFSUrl(factPathFromUser))
      }
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
      carbonLoadModel.setDatabaseName(relation.tableMeta.carbonTableIdentifier.getDatabaseName)
      carbonLoadModel.setStorePath(relation.tableMeta.storePath)

      val table = relation.tableMeta.carbonTable
      carbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
      carbonLoadModel.setTableName(table.getFactTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)

      val partitionLocation = relation.tableMeta.storePath + "/partition/" +
                              relation.tableMeta.carbonTableIdentifier.getDatabaseName + "/" +
                              relation.tableMeta.carbonTableIdentifier.getTableName + "/"


      val columnar = sparkSession.conf.get("carbon.is.columnar.storage", "true").toBoolean

      // TODO It will be removed after kettle is removed.
      val useKettle = options.get("use_kettle") match {
        case Some(value) => value.toBoolean
        case _ =>
          var useKettleLocal = System.getProperty("use_kettle")
          if (useKettleLocal == null && sparkSession.sparkContext.getConf.contains("use_kettle")) {
            useKettleLocal = sparkSession.sparkContext.getConf.get("use_kettle")
          }
          if (useKettleLocal == null) {
            useKettleLocal = CarbonProperties.getInstance().
              getProperty(CarbonCommonConstants.USE_KETTLE,
                CarbonCommonConstants.USE_KETTLE_DEFAULT)
          }
          try {
            useKettleLocal.toBoolean
          } catch {
            case e: Exception => CarbonCommonConstants.USE_KETTLE_DEFAULT.toBoolean
          }
      }

      val kettleHomePath =
        if (useKettle) CarbonScalaUtil.getKettleHome(sparkSession.sqlContext) else ""

      val delimiter = options.getOrElse("delimiter", ",")
      val quoteChar = options.getOrElse("quotechar", "\"")
      val fileHeader = options.getOrElse("fileheader", "")
      val escapeChar = options.getOrElse("escapechar", "\\")
      val commentchar = options.getOrElse("commentchar", "#")
      val columnDict = options.getOrElse("columndict", null)
      val serializationNullFormat = options.getOrElse("serialization_null_format", "\\N")
      val badRecordsLoggerEnable = options.getOrElse("bad_records_logger_enable", "false")
      val badRecordsLoggerRedirect = options.getOrElse("bad_records_action", "force")
      val allDictionaryPath = options.getOrElse("all_dictionary_path", "")
      val complex_delimiter_level_1 = options.getOrElse("complex_delimiter_level_1", "\\$")
      val complex_delimiter_level_2 = options.getOrElse("complex_delimiter_level_2", "\\:")
      val dateFormat = options.getOrElse("dateformat", null)
      validateDateFormat(dateFormat, table)
      val maxColumns = options.getOrElse("maxcolumns", null)
      carbonLoadModel.setMaxColumns(checkDefaultValue(maxColumns, null))
      carbonLoadModel.setEscapeChar(checkDefaultValue(escapeChar, "\\"))
      carbonLoadModel.setQuoteChar(checkDefaultValue(quoteChar, "\""))
      carbonLoadModel.setCommentChar(checkDefaultValue(commentchar, "#"))
      carbonLoadModel.setDateFormat(dateFormat)
      carbonLoadModel.setDefaultTimestampFormat(CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
      carbonLoadModel.setDefaultDateFormat(CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
      carbonLoadModel
        .setSerializationNullFormat(
          TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + "," + serializationNullFormat)
      carbonLoadModel
        .setBadRecordsLoggerEnable(
          TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + "," + badRecordsLoggerEnable)
      carbonLoadModel
        .setBadRecordsAction(
          TableOptionConstant.BAD_RECORDS_ACTION.getName + "," + badRecordsLoggerRedirect)

      val useOnePass = options.getOrElse("single_pass", "false").trim.toLowerCase match {
        case "true" =>
          if (!useKettle && StringUtils.isEmpty(allDictionaryPath)) {
            true
          } else {
            LOGGER.error("Can't use single_pass, because SINGLE_PASS and ALL_DICTIONARY_PATH" +
                         "can not be used together, and USE_KETTLE must be set as false")
            false
          }
        case "false" =>
          false
        case illegal =>
          LOGGER.error(s"Can't use single_pass, because illegal syntax found: [" + illegal + "] " +
                       "Please set it as 'true' or 'false'")
          false
      }
      carbonLoadModel.setUseOnePass(useOnePass)
      if (delimiter.equalsIgnoreCase(complex_delimiter_level_1) ||
          complex_delimiter_level_1.equalsIgnoreCase(complex_delimiter_level_2) ||
          delimiter.equalsIgnoreCase(complex_delimiter_level_2)) {
        sys.error(s"Field Delimiter & Complex types delimiter are same")
      }
      else {
        carbonLoadModel.setComplexDelimiterLevel1(
          CarbonUtil.delimiterConverter(complex_delimiter_level_1))
        carbonLoadModel.setComplexDelimiterLevel2(
          CarbonUtil.delimiterConverter(complex_delimiter_level_2))
      }
      // set local dictionary path, and dictionary file extension
      carbonLoadModel.setAllDictPath(allDictionaryPath)

      val partitionStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      var result: Future[DictionaryServer] = null
      var executorService: ExecutorService = null

      try {
        // First system has to partition the data first and then call the load data
        LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
        carbonLoadModel.setFactFilePath(factPath)
        carbonLoadModel.setCsvDelimiter(CarbonUtil.unescapeChar(delimiter))
        carbonLoadModel.setCsvHeader(fileHeader)
        carbonLoadModel.setColDictFilePath(columnDict)
        carbonLoadModel.setDirectLoad(true)
        carbonLoadModel.setCsvHeaderColumns(CommonUtil.getCsvHeaderColumns(carbonLoadModel))
        GlobalDictionaryUtil.updateTableMetadataFunc = LoadTable.updateTableMetadata

        if (carbonLoadModel.getUseOnePass) {
          val colDictFilePath = carbonLoadModel.getColDictFilePath
          if (colDictFilePath != null) {
            val storePath = relation.tableMeta.storePath
            val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
            val carbonTableIdentifier = carbonTable.getAbsoluteTableIdentifier
              .getCarbonTableIdentifier
            val carbonTablePath = CarbonStorePath
              .getCarbonTablePath(storePath, carbonTableIdentifier)
            val dictFolderPath = carbonTablePath.getMetadataDirectoryPath
            val dimensions = carbonTable.getDimensionByTableName(
              carbonTable.getFactTableName).asScala.toArray
            carbonLoadModel.initPredefDictMap()
            // generate predefined dictionary
            GlobalDictionaryUtil
              .generatePredefinedColDictionary(colDictFilePath, carbonTableIdentifier,
                dimensions, carbonLoadModel, sparkSession.sqlContext, storePath, dictFolderPath)
          }
          // dictionaryServerClient dictionary generator
          val dictionaryServerPort = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.DICTIONARY_SERVER_PORT,
              CarbonCommonConstants.DICTIONARY_SERVER_PORT_DEFAULT)
          carbonLoadModel.setDictionaryServerPort(Integer.parseInt(dictionaryServerPort))
          val sparkDriverHost = sparkSession.sqlContext.sparkContext.
            getConf.get("spark.driver.host")
          carbonLoadModel.setDictionaryServerHost(sparkDriverHost)
          // start dictionary server when use one pass load.
          executorService = Executors.newFixedThreadPool(1)
          result = executorService.submit(new Callable[DictionaryServer]() {
            @throws[Exception]
            def call: DictionaryServer = {
              Thread.currentThread().setName("Dictionary server")
              val server: DictionaryServer = new DictionaryServer
              server.startServer(dictionaryServerPort.toInt)
              server
            }
          })
          CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
            carbonLoadModel,
            relation.tableMeta.storePath,
            kettleHomePath,
            columnar,
            partitionStatus,
            useKettle,
            result,
            dataFrame,
            updateModel)
        }
        else {
          val (dictionaryDataFrame, loadDataFrame) = if (updateModel.isDefined) {
            val fields = dataFrame.get.schema.fields
            import org.apache.spark.sql.functions.udf
            // extracting only segment from tupleId
            val getSegIdUDF = udf((tupleId: String) =>
              CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
            // getting all fields except tupleId field as it is not required in the value
            var otherFields = fields.toSeq
              .filter(field => !field.name
                .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
              .map(field => {
                if (field.name.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION) && false) {
                  new Column(field.name
                    .substring(0,
                      field.name.lastIndexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION)))
                } else {

                  new Column(field.name)
                }
              })

            // extract tupleId field which will be used as a key
            val segIdColumn = getSegIdUDF(new Column(UnresolvedAttribute
              .quotedString(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))).as("segId")
            // use dataFrameWithoutTupleId as dictionaryDataFrame
            val dataFrameWithoutTupleId = dataFrame.get.select(otherFields: _*)
            otherFields = otherFields :+ segIdColumn
            // use dataFrameWithTupleId as loadDataFrame
            val dataFrameWithTupleId = dataFrame.get.select(otherFields: _*)
            (Some(dataFrameWithoutTupleId), Some(dataFrameWithTupleId))
          } else {
            (dataFrame, dataFrame)
          }
          GlobalDictionaryUtil
            .generateGlobalDictionary(
              sparkSession.sqlContext,
              carbonLoadModel,
              relation.tableMeta.storePath,
              dictionaryDataFrame)
          CarbonDataRDDFactory.loadCarbonData(sparkSession.sqlContext,
            carbonLoadModel,
            relation.tableMeta.storePath,
            kettleHomePath,
            columnar,
            partitionStatus,
            useKettle,
            result,
            loadDataFrame,
            updateModel)
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex)
          LOGGER.audit(s"Dataload failure for $dbName.$tableName. Please check the logs")
          throw ex
      } finally {
        // Once the data load is successful delete the unwanted partition files
        try {

          // shutdown dictionary server thread
          if (carbonLoadModel.getUseOnePass) {
            executorService.shutdownNow()
          }

          val fileType = FileFactory.getFileType(partitionLocation)
          if (FileFactory.isFileExist(partitionLocation, fileType)) {
            val file = FileFactory
              .getCarbonFile(partitionLocation, fileType)
            CarbonUtil.deleteFoldersAndFiles(file)
          }
        } catch {
          case ex: Exception =>
            LOGGER.error(ex)
            LOGGER.audit(s"Dataload failure for $dbName.$tableName. " +
                         "Problem deleting the partition folder")
            throw ex
        }

      }
    } catch {
      case dle: DataLoadingException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + dle.getMessage)
        throw dle
      case mce: MalformedCarbonCommandException =>
        LOGGER.audit(s"Dataload failed for $dbName.$tableName. " + mce.getMessage)
        throw mce
    } finally {
      if (carbonLock != null) {
        if (carbonLock.unlock()) {
          LOGGER.info("Table MetaData Unlocked Successfully after data load")
        } else {
          LOGGER.error("Unable to unlock Table MetaData")
        }
      }
    }
    Seq.empty
  }

  private def validateDateFormat(dateFormat: String, table: CarbonTable): Unit = {
    val dimensions = table.getDimensionByTableName(tableName).asScala
    if (dateFormat != null) {
      if (dateFormat.trim == "") {
        throw new MalformedCarbonCommandException("Error: Option DateFormat is set an empty " +
                                                  "string.")
      } else {
        val dateFormats: Array[String] = dateFormat.split(CarbonCommonConstants.COMMA)
        for (singleDateFormat <- dateFormats) {
          val dateFormatSplits: Array[String] = singleDateFormat.split(":", 2)
          val columnName = dateFormatSplits(0).trim.toLowerCase
          if (!dimensions.exists(_.getColName.equals(columnName))) {
            throw new MalformedCarbonCommandException("Error: Wrong Column Name " +
                                                      dateFormatSplits(0) +
                                                      " is provided in Option DateFormat.")
          }
          if (dateFormatSplits.length < 2 || dateFormatSplits(1).trim.isEmpty) {
            throw new MalformedCarbonCommandException("Error: Option DateFormat is not provided " +
                                                      "for " + "Column " + dateFormatSplits(0) +
                                                      ".")
          }
        }
      }
    }
  }
}

private[sql] case class DeleteLoadByDate(
    databaseNameOp: Option[String],
    tableName: String,
    dateField: String,
    loadDate: String) {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    CarbonStore.deleteLoadByDate(
      loadDate,
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName
    )
    Seq.empty
  }

}

case class CleanFiles(
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
    CarbonStore.cleanFiles(
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      relation.asInstanceOf[CarbonRelation].tableMeta.storePath
    )
    Seq.empty
  }
}

case class ShowLoads(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[String],
    override val output: Seq[Attribute]) extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    CarbonStore.showSegments(
      getDB.getDatabaseName(databaseNameOp, sparkSession),
      tableName,
      limit
    )
  }
}

case class CarbonDropTableCommand(ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String)
  extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = getDB.getDatabaseName(databaseNameOp, sparkSession)
    val identifier = TableIdentifier(tableName, Option(dbName))
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName, "")
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(carbonTableIdentifier, LockUsage.DROP_TABLE_LOCK)
    val storePath = CarbonEnv.get.carbonMetastore.storePath
    var isLocked = false
    try {
      isLocked = carbonLock.lockWithRetries()
      if (isLocked) {
        logInfo("Successfully able to get the lock for drop.")
      }
      else {
        LOGGER.audit(s"Dropping table $dbName.$tableName failed as the Table is locked")
        sys.error("Table is locked for deletion. Please try after some time")
      }
      LOGGER.audit(s"Deleting table [$tableName] under database [$dbName]")
      CarbonEnv.get.carbonMetastore.dropTable(storePath, identifier)(sparkSession)
      LOGGER.audit(s"Deleted table [$tableName] under database [$dbName]")
    } finally {
      if (carbonLock != null && isLocked) {
        if (carbonLock.unlock()) {
          logInfo("Table MetaData Unlocked Successfully after dropping the table")
          // deleting any remaining files.
          val metadataFilePath = CarbonStorePath
            .getCarbonTablePath(storePath, carbonTableIdentifier).getMetadataDirectoryPath
          val fileType = FileFactory.getFileType(metadataFilePath)
          if (FileFactory.isFileExist(metadataFilePath, fileType)) {
            val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
            CarbonUtil.deleteFoldersAndFiles(file.getParentFile)
          }
        }
        // delete bad record log after drop table
        val badLogPath = CarbonUtil.getBadLogPath(dbName + File.separator + tableName)
        val badLogFileType = FileFactory.getFileType(badLogPath)
        if (FileFactory.isFileExist(badLogPath, badLogFileType)) {
          val file = FileFactory.getCarbonFile(badLogPath, badLogFileType)
          CarbonUtil.deleteFoldersAndFiles(file)
        }
      }
    }
    Seq.empty
  }
}

private[sql] case class DescribeCommandFormatted(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.get.carbonMetastore
      .lookupRelation(tblIdentifier)(sparkSession).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    val dims = relation.metaData.dims.map(x => x.toLowerCase)
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val fieldName = field.name.toLowerCase
      val comment = if (dims.contains(fieldName)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
          relation.tableMeta.carbonTableIdentifier.getTableName,
          fieldName)
        if (null != dimension.getColumnProperties && dimension.getColumnProperties.size() > 0) {
          colProps.append(fieldName).append(".")
            .append(mapper.writeValueAsString(dimension.getColumnProperties))
            .append(",")
        }
        if (dimension.hasEncoding(Encoding.DICTIONARY) &&
            !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          "DICTIONARY, KEY COLUMN"
        } else {
          "KEY COLUMN"
        }
      } else {
        "MEASURE"
      }
      (field.name, field.dataType.simpleString, comment)
    }
    val colPropStr = if (colProps.toString().trim().length() > 0) {
      // drops additional comma at end
      colProps.toString().dropRight(1)
    } else {
      colProps.toString()
    }
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Database Name: ", relation.tableMeta.carbonTableIdentifier
      .getDatabaseName, "")
    )
    results ++= Seq(("Table Name: ", relation.tableMeta.carbonTableIdentifier.getTableName, ""))
    results ++= Seq(("CARBON Store Path: ", relation.tableMeta.storePath, ""))
    val carbonTable = relation.tableMeta.carbonTable
    results ++= Seq(("Table Block Size : ", carbonTable.getBlockSizeInMB + " MB", ""))
    results ++= Seq(("", "", ""), ("##Detailed Column property", "", ""))
    if (colPropStr.length() > 0) {
      results ++= Seq((colPropStr, "", ""))
    } else {
      results ++= Seq(("ADAPTIVE", "", ""))
    }
    val dimension = carbonTable
      .getDimensionByTableName(relation.tableMeta.carbonTableIdentifier.getTableName)
    results ++= getColumnGroups(dimension.asScala.toList)
    results.map { case (name, dataType, comment) =>
      Row(f"$name%-36s", f"$dataType%-80s", f"$comment%-72s")
    }
  }

  private def getColumnGroups(dimensions: List[CarbonDimension]): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] =
      Seq(("", "", ""), ("##Column Group Information", "", ""))
    val groupedDimensions = dimensions.groupBy(x => x.columnGroupId()).filter {
      case (groupId, _) => groupId != -1
    }.toSeq.sortBy(_._1)
    val groups = groupedDimensions.map(colGroups => {
      colGroups._2.map(dim => dim.getColName).mkString(", ")
    })
    var index = 1
    groups.foreach { x =>
      results = results :+ (s"Column Group $index", x, "")
      index = index + 1
    }
    results
  }
}