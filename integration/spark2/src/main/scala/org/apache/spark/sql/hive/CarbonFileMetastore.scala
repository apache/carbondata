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

package org.apache.spark.sql.hive

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.carbondata.core.fileoperations.FileWriteOperation
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier, schema}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.stats.{QueryStatistic, QueryStatisticsConstants}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, CarbonUtil}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.processing.merger.TableMeta
import org.apache.carbondata.spark.util.CarbonSparkUtil

case class MetaData(var tablesMeta: ArrayBuffer[TableMeta]) {
  // clear the metadata
  def clear(): Unit = {
    tablesMeta.clear()
  }
}

case class CarbonMetaData(dims: Seq[String],
    msrs: Seq[String],
    carbonTable: CarbonTable,
    dictionaryMap: DictionaryMap)

case class DictionaryMap(dictionaryMap: Map[String, Boolean]) {
  def get(name: String): Option[Boolean] = {
    dictionaryMap.get(name.toLowerCase)
  }
}

class CarbonFileMetastore(conf: RuntimeConfig, val storePath: String) extends CarbonMetaStore {

  @transient
  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore
    .put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, System.currentTimeMillis())

  private val nextId = new AtomicLong(0)

  def nextQueryId: String = {
    System.nanoTime() + ""
  }

  val metadata = MetaData(new ArrayBuffer[TableMeta]())


  /**
   * Create spark session from paramters.
   *
   * @param parameters
   * @param absIdentifier
   * @param sparkSession
   */
  override def createCarbonRelation(parameters: Map[String, String],
      absIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): CarbonRelation = {
    lookupRelation(TableIdentifier(absIdentifier.getCarbonTableIdentifier.getTableName,
      Some(absIdentifier.getCarbonTableIdentifier.getDatabaseName)))(sparkSession)
      .asInstanceOf[CarbonRelation]
  }

  def lookupRelation(dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): LogicalPlan = {
    lookupRelation(TableIdentifier(tableName, dbName))(sparkSession)
  }

  def lookupRelation(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(
      sparkSession.catalog.currentDatabase
    )
    val tables = getTableFromMetadata(database, tableIdentifier.table, true)
    tables match {
      case Some(t) =>
        CarbonRelation(database, tableIdentifier.table,
          CarbonSparkUtil.createSparkMeta(t.carbonTable), t)
      case None =>
        readCarbonSchema(database, tableIdentifier.table) match {
          case Some(t) =>
            CarbonRelation(database, tableIdentifier.table,
              CarbonSparkUtil.createSparkMeta(t.carbonTable), t)
          case None =>
            throw new NoSuchTableException(database, tableIdentifier.table)
        }
    }
  }

  /**
   * This method will search for a table in the catalog metadata
   *
   * @param database
   * @param tableName
   * @return
   */
  def getTableFromMetadata(database: String,
      tableName: String, readStore: Boolean = false): Option[TableMeta] = {
    metadata.tablesMeta
      .find(c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
                 c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableName))
  }

  def tableExists(
      table: String,
      databaseOp: Option[String] = None)(sparkSession: SparkSession): Boolean = {
   tableExists(TableIdentifier(table, databaseOp))(sparkSession)
  }

  override def tableExists(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): Boolean = {
    try {
      lookupRelation(tableIdentifier)(sparkSession)
    } catch {
      case e: Exception =>
        return false
    }
    true
  }

  private def readCarbonSchema(dbName: String,
      tableName: String) :Option[TableMeta] = {
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName.toLowerCase(),
      tableName.toLowerCase(), UUID.randomUUID().toString)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val tableMetadataFile = carbonTablePath.getSchemaFilePath
    val fileType = FileFactory.getFileType(tableMetadataFile)
    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      val tableUniqueName = dbName + "_" + tableName
      val tableInfo: TableInfo = CarbonUtil.readSchemaFile(tableMetadataFile)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl
      val wrapperTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, storePath)
      val schemaFilePath = CarbonStorePath
        .getCarbonTablePath(storePath, carbonTableIdentifier).getSchemaFilePath
      wrapperTableInfo.setStorePath(storePath)
      wrapperTableInfo
        .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
      val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
      val tableMeta = new TableMeta(carbonTable.getCarbonTableIdentifier, storePath,
        carbonTable)
      metadata.tablesMeta += tableMeta
      Some(tableMeta)
    } else {
      None
    }
  }

  /**
   * This method will overwrite the existing schema and update it with the given details
   *
   * @param newTableIdentifier
   * @param thriftTableInfo
   * @param schemaEvolutionEntry
   * @param carbonStorePath
   * @param sparkSession
   */
  def updateTableSchema(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      carbonStorePath: String)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    if (schemaEvolutionEntry != null) {
      thriftTableInfo.fact_table.schema_evolution.schema_evolution_history.add(schemaEvolutionEntry)
    }
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
          newTableIdentifier.getDatabaseName,
          newTableIdentifier.getTableName,
          carbonStorePath)
    val identifier =
      new CarbonTableIdentifier(newTableIdentifier.getDatabaseName,
        newTableIdentifier.getTableName,
        wrapperTableInfo.getFactTable.getTableId)
    val path = createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      identifier)
    addTableCache(wrapperTableInfo, identifier)
    path
  }

  /**
   * This method will is used to remove the evolution entry in case of failure.
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param carbonStorePath
   * @param sparkSession
   */
  def revertTableSchema(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      carbonStorePath: String)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
        carbonTableIdentifier.getDatabaseName,
        carbonTableIdentifier.getTableName,
        carbonStorePath)
    val evolutionEntries = thriftTableInfo.fact_table.schema_evolution.schema_evolution_history
    evolutionEntries.remove(evolutionEntries.size() - 1)
    val identifier =
      new CarbonTableIdentifier(carbonTableIdentifier.getDatabaseName,
        carbonTableIdentifier.getTableName,
        wrapperTableInfo.getFactTable.getTableId)
    val path = createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      identifier)
    addTableCache(wrapperTableInfo, identifier)
    path
  }



  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableInfo
   *
   */
  def saveToDisk(tableInfo: schema.table.TableInfo) {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val dbName = tableInfo.getDatabaseName
    val tableName = tableInfo.getFactTable.getTableName
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val identifier = new CarbonTableIdentifier(dbName,
        tableName,
        tableInfo.getFactTable.getTableId)
    val carbonTablePath = createSchemaThriftFile(tableInfo,
      thriftTableInfo,
      identifier)
    LOGGER.info(s"Table $tableName for Database $dbName created successfully.")
  }

  /**
   * Generates schema string from TableInfo
   */
  override def generateTableSchemaString(tableInfo: schema.table.TableInfo, dbName: String,
      tableName: String): (String, String) = {
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName,
      tableInfo.getFactTable.getTableId)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaMetadataPath =
      CarbonTablePath.getFolderContainingFile(carbonTablePath.getSchemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    val schemaEvolutionEntry = new schema.SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvalution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    removeTableFromMetadata(dbName, tableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    addTableCache(tableInfo, carbonTableIdentifier)
    (carbonTablePath.getPath, CarbonUtil.convertToMultiGsonStrings(tableInfo, " ", "", ","))
  }

  /**
   * This method will write the schema thrift file in carbon store and load table metadata
   *
   * @param tableInfo
   * @param thriftTableInfo
   * @return
   */
  private def createSchemaThriftFile(tableInfo: schema.table.TableInfo,
      thriftTableInfo: TableInfo,
      carbonTableIdentifier: CarbonTableIdentifier): String = {
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setStorePath(storePath)
    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open(FileWriteOperation.OVERWRITE)
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(carbonTableIdentifier.getDatabaseName,
      carbonTableIdentifier.getTableName))
    carbonTablePath.getPath
  }

  protected def addTableCache(tableInfo: table.TableInfo,
      identifier: CarbonTableIdentifier) = {
    removeTableFromMetadata(identifier.getDatabaseName, identifier.getTableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    val tableMeta = new TableMeta(identifier, storePath,
      CarbonMetadata.getInstance().getCarbonTable(identifier.getTableUniqueName))
    metadata.tablesMeta += tableMeta
  }

  /**
   * This method will remove the table meta from catalog metadata array
   *
   * @param dbName
   * @param tableName
   */
  def removeTableFromMetadata(dbName: String, tableName: String): Unit = {
    val metadataToBeRemoved: Option[TableMeta] = getTableFromMetadata(dbName, tableName)
    metadataToBeRemoved match {
      case Some(tableMeta) =>
        metadata.tablesMeta -= tableMeta
        CarbonMetadata.getInstance.removeTable(dbName + "_" + tableName)
      case None =>
        if (LOGGER.isDebugEnabled) {
          LOGGER.debug(s"No entry for table $tableName in database $dbName")
        }
    }
  }

  private def updateMetadataByWrapperTable(
      wrapperTableInfo: org.apache.carbondata.core.metadata.schema.table.TableInfo): Unit = {

    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable(
      wrapperTableInfo.getTableUniqueName)
    for (i <- metadata.tablesMeta.indices) {
      if (wrapperTableInfo.getTableUniqueName.equals(
        metadata.tablesMeta(i).carbonTableIdentifier.getTableUniqueName)) {
        metadata.tablesMeta(i).carbonTable = carbonTable
      }
    }
  }

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, storePath: String): Unit = {

    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis())
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, storePath)
    wrapperTableInfo
      .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
    wrapperTableInfo.setStorePath(storePath)
    updateMetadataByWrapperTable(wrapperTableInfo)
  }


  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    val dbName = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val tableName = tableIdentifier.table.toLowerCase

    val tablePath = CarbonStorePath.getCarbonTablePath(this.storePath,
      new CarbonTableIdentifier(dbName, tableName, "")).getPath

    val fileType = FileFactory.getFileType(tablePath)
    FileFactory.isFileExist(tablePath, fileType)
  }

  def dropTable(tableStorePath: String, tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession) {
    val dbName = tableIdentifier.database.get
    val tableName = tableIdentifier.table

    val metadataFilePath = CarbonStorePath.getCarbonTablePath(tableStorePath,
      new CarbonTableIdentifier(dbName, tableName, "")).getMetadataDirectoryPath
    val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
    if (null != carbonTable) {
      // clear driver B-tree and dictionary cache
      ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(carbonTable)
    }
    val fileType = FileFactory.getFileType(metadataFilePath)

    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      checkSchemasModifiedTimeAndReloadTables

      removeTableFromMetadata(dbName, tableName)
      updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
      CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
      // discard cached table info in cachedDataSourceTables
      sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
    }
  }

  private def getTimestampFileAndType(databaseName: String, tableName: String) = {
    val timestampFile = storePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    val timestampFileType = FileFactory.getFileType(timestampFile)
    (timestampFile, timestampFileType)
  }

  /**
   * This method will put the updated timestamp of schema file in the table modified time store map
   *
   * @param timeStamp
   */
  private def updateSchemasUpdatedTime(timeStamp: Long) {
    tableModifiedTimeStore.put("default", timeStamp)
  }

  def updateAndTouchSchemasUpdatedTime(databaseName: String, tableName: String) {
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(databaseName, tableName))
  }

  /**
   * This method will read the timestamp of empty schema file
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  private def readSchemaFileSystemTime(databaseName: String, tableName: String): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime
    } else {
      System.currentTimeMillis()
    }
  }

  /**
   * This method will check and create an empty schema timestamp file
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  private def touchSchemaFileSystemTime(databaseName: String, tableName: String): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType(databaseName, tableName)
    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $databaseName.$tableName")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }
    val systemTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(systemTime)
    systemTime
  }

  def checkSchemasModifiedTimeAndReloadTables() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType("", "")
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
        getLastModifiedTime ==
            tableModifiedTimeStore.get(CarbonCommonConstants.DATABASE_DEFAULT_NAME))) {
        refreshCache()
      }
    }
  }

  private def refreshCache() {
    metadata.tablesMeta.clear()
  }

  override def isReadFromHiveMetaStore: Boolean = false

  override def listAllTables(sparkSession: SparkSession): Seq[CarbonTable] =
    metadata.tablesMeta.map(_.carbonTable)

  override def getThriftTableInfo(tablePath: CarbonTablePath)
    (sparkSession: SparkSession): TableInfo = {
    val tableMetadataFile = tablePath.getSchemaFilePath
    CarbonUtil.readSchemaFile(tableMetadataFile)
  }
}

