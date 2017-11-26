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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.FileWriteOperation
import org.apache.carbondata.core.metadata.{schema, AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.events.{LookupRelationPostEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.util.CarbonSparkUtil

case class MetaData(var carbonTables: ArrayBuffer[CarbonTable]) {
  // clear the metadata
  def clear(): Unit = {
    carbonTables.clear()
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

class CarbonFileMetastore extends CarbonMetaStore {

  @transient
  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore
    .put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, System.currentTimeMillis())

  private val nextId = new AtomicLong(0)

  def nextQueryId: String = {
    System.nanoTime() + ""
  }

  val metadata = MetaData(new ArrayBuffer[CarbonTable]())


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
    val database = absIdentifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = absIdentifier.getCarbonTableIdentifier.getTableName
    val tables = getTableFromMetadataCache(database, tableName)
    tables match {
      case Some(t) =>
        CarbonRelation(database, tableName, CarbonSparkUtil.createSparkMeta(t), t)
      case None =>
        readCarbonSchema(absIdentifier) match {
          case Some(meta) =>
            CarbonRelation(database, tableName,
              CarbonSparkUtil.createSparkMeta(meta), meta)
          case None =>
            throw new NoSuchTableException(database, tableName)
        }
    }
  }

  /**
   * This method will overwrite the existing schema and update it with the given details
   *
   * @param newTableIdentifier
   * @param thriftTableInfo
   * @param carbonStorePath
   * @param sparkSession
   */
  def updateTableSchemaForDataMap(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      carbonStorePath: String)(sparkSession: SparkSession): String = {
    updateTableSchemaForAlter(newTableIdentifier,
      oldTableIdentifier, thriftTableInfo, null, carbonStorePath) (sparkSession)
  }

  def lookupRelation(dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): LogicalPlan = {
    lookupRelation(TableIdentifier(tableName, dbName))(sparkSession)
  }

  override def lookupRelation(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): LogicalPlan = {
    val database = tableIdentifier.database.getOrElse(
      sparkSession.catalog.currentDatabase)
    val relation = sparkSession.sessionState.catalog.lookupRelation(tableIdentifier) match {
      case SubqueryAlias(_,
      LogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _),
      _) =>
        carbonDatasourceHadoopRelation.carbonRelation
      case LogicalRelation(
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        carbonDatasourceHadoopRelation.carbonRelation
      case _ => throw new NoSuchTableException(database, tableIdentifier.table)
    }

    // fire post event after lookup relation
    val operationContext = new OperationContext
    val lookupRelationPostEvent: LookupRelationPostEvent =
      LookupRelationPostEvent(
        relation.carbonTable,
        sparkSession)
    OperationListenerBus.getInstance.fireEvent(lookupRelationPostEvent, operationContext)
    relation
  }

  /**
   * This method will search for a table in the catalog metadata
   *
   * @param database
   * @param tableName
   * @return
   */
  def getTableFromMetadataCache(database: String, tableName: String): Option[CarbonTable] = {
    metadata.carbonTables
      .find(table => table.getDatabaseName.equalsIgnoreCase(database) &&
        table.getTableName.equalsIgnoreCase(tableName))
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

  private def readCarbonSchema(identifier: AbsoluteTableIdentifier): Option[CarbonTable] = {
    val dbName = identifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = identifier.getCarbonTableIdentifier.getTableName
    val tablePath = identifier.getTablePath
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName.toLowerCase(),
      tableName.toLowerCase(), UUID.randomUUID().toString)
    val carbonTablePath =
      CarbonStorePath.getCarbonTablePath(tablePath, carbonTableIdentifier)
    val tableMetadataFile = carbonTablePath.getSchemaFilePath
    val fileType = FileFactory.getFileType(tableMetadataFile)
    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      val tableUniqueName = dbName + "_" + tableName
      val tableInfo: TableInfo = CarbonUtil.readSchemaFile(tableMetadataFile)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl
      val wrapperTableInfo = schemaConverter
        .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath)
      val schemaFilePath = CarbonStorePath
        .getCarbonTablePath(tablePath, carbonTableIdentifier).getSchemaFilePath
      wrapperTableInfo.setTablePath(tablePath)
      wrapperTableInfo
        .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
      CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
      val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
      metadata.carbonTables += carbonTable
      Some(carbonTable)
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
   * @param tablePath
   * @param sparkSession
   */
  def updateTableSchemaForAlter(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      tablePath: String) (sparkSession: SparkSession): String = {
    val absoluteTableIdentifier = new AbsoluteTableIdentifier(tablePath, oldTableIdentifier)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    if (schemaEvolutionEntry != null) {
      thriftTableInfo.fact_table.schema_evolution.schema_evolution_history.add(schemaEvolutionEntry)
    }
    val oldCarbonTablePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier)
    val newAbsoluteTableIdentifier = new AbsoluteTableIdentifier(CarbonUtil
      .getNewTablePath(oldCarbonTablePath, newTableIdentifier), newTableIdentifier)
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
        newTableIdentifier.getDatabaseName,
        newTableIdentifier.getTableName,
        newAbsoluteTableIdentifier.getTablePath)
    val identifier =
      new CarbonTableIdentifier(newTableIdentifier.getDatabaseName,
        newTableIdentifier.getTableName,
        wrapperTableInfo.getFactTable.getTableId)
    val path = createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      identifier)
    addTableCache(wrapperTableInfo, newAbsoluteTableIdentifier)
    path
  }

  /**
   * This method will is used to remove the evolution entry in case of failure.
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param sparkSession
   */
  def revertTableSchemaInAlterFailure(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier)(sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
        carbonTableIdentifier.getDatabaseName,
        carbonTableIdentifier.getTableName,
        absoluteTableIdentifier.getTablePath)
    val evolutionEntries = thriftTableInfo.fact_table.schema_evolution.schema_evolution_history
    evolutionEntries.remove(evolutionEntries.size() - 1)
    wrapperTableInfo.setTablePath(absoluteTableIdentifier.getTablePath)
    val path = createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      absoluteTableIdentifier.getCarbonTableIdentifier)
    addTableCache(wrapperTableInfo, absoluteTableIdentifier)
    path
  }

  override def revertTableSchemaForPreAggCreationFailure(absoluteTableIdentifier:
  AbsoluteTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
        absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
        absoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
        absoluteTableIdentifier.getTablePath)
    val childSchemaList = wrapperTableInfo.getDataMapSchemaList
    childSchemaList.remove(childSchemaList.size() - 1)
    wrapperTableInfo.setTablePath(absoluteTableIdentifier.getTablePath)
    val path = createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      absoluteTableIdentifier.getCarbonTableIdentifier)
    addTableCache(wrapperTableInfo, absoluteTableIdentifier)
    path

  }

  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableInfo
   *
   */
  def saveToDisk(tableInfo: schema.table.TableInfo, tablePath: String) {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val dbName = tableInfo.getDatabaseName
    val tableName = tableInfo.getFactTable.getTableName
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    val identifier = AbsoluteTableIdentifier.from(tablePath, dbName, tableName)
    tableInfo.setTablePath(identifier.getTablePath)
    createSchemaThriftFile(tableInfo,
      thriftTableInfo,
      identifier.getCarbonTableIdentifier)
    LOGGER.info(s"Table $tableName for Database $dbName created successfully.")
  }

  /**
   * Generates schema string from TableInfo
   */
  override def generateTableSchemaString(tableInfo: schema.table.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): String = {
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier)
    val schemaMetadataPath =
      CarbonTablePath.getFolderContainingFile(carbonTablePath.getSchemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    tableInfo.setTablePath(absoluteTableIdentifier.getTablePath)
    val schemaEvolutionEntry = new schema.SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvalution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    removeTableFromMetadata(tableInfo.getDatabaseName, tableInfo.getFactTable.getTableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    addTableCache(tableInfo, absoluteTableIdentifier)
    CarbonUtil.convertToMultiGsonStrings(tableInfo, " ", "", ",")
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
    val carbonTablePath = CarbonStorePath.
      getCarbonTablePath(tableInfo.getTablePath, carbonTableIdentifier)
    val schemaFilePath = carbonTablePath.getSchemaFilePath
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    tableInfo.setMetaDataFilepath(schemaMetadataPath)
    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType)
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open(FileWriteOperation.OVERWRITE)
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    updateSchemasUpdatedTime(touchSchemaFileSystemTime())
    carbonTablePath.getPath
  }

  protected def addTableCache(tableInfo: table.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier) = {
    val identifier = absoluteTableIdentifier.getCarbonTableIdentifier
    CarbonMetadata.getInstance.removeTable(tableInfo.getTableUniqueName)
    removeTableFromMetadata(identifier.getDatabaseName, identifier.getTableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    metadata.carbonTables +=
      CarbonMetadata.getInstance().getCarbonTable(identifier.getTableUniqueName)
  }

  /**
   * This method will remove the table meta from catalog metadata array
   *
   * @param dbName
   * @param tableName
   */
  def removeTableFromMetadata(dbName: String, tableName: String): Unit = {
    val carbonTableToBeRemoved: Option[CarbonTable] = getTableFromMetadataCache(dbName, tableName)
    carbonTableToBeRemoved match {
      case Some(carbonTable) =>
        metadata.carbonTables -= carbonTable
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
    for (i <- metadata.carbonTables.indices) {
      if (wrapperTableInfo.getTableUniqueName.equals(
        metadata.carbonTables(i).getTableUniqueName)) {
        metadata.carbonTables(i) = carbonTable
      }
    }
  }

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, tablePath: String): Unit = {

    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis())
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath)
    wrapperTableInfo
      .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
    wrapperTableInfo.setTablePath(tablePath)
    updateMetadataByWrapperTable(wrapperTableInfo)
  }


  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    try {
      val tablePath = lookupRelation(tableIdentifier)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable.getTablePath
      val fileType = FileFactory.getFileType(tablePath)
      FileFactory.isFileExist(tablePath, fileType)
    } catch {
      case e: Exception =>
       false
    }
  }


  def dropTable(absoluteTableIdentifier: AbsoluteTableIdentifier)
    (sparkSession: SparkSession) {
    val dbName = absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = absoluteTableIdentifier.getCarbonTableIdentifier.getTableName
    val metadataFilePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier)
      .getMetadataDirectoryPath
    val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
    if (null != carbonTable) {
      // clear driver B-tree and dictionary cache
      ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(carbonTable)
    }
    val fileType = FileFactory.getFileType(metadataFilePath)

    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      checkSchemasModifiedTimeAndReloadTables()

      removeTableFromMetadata(dbName, tableName)
      updateSchemasUpdatedTime(touchSchemaFileSystemTime())
      CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
      // discard cached table info in cachedDataSourceTables
      val tableIdentifier = TableIdentifier(tableName, Option(dbName))
      sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
      DataMapStoreManager.getInstance().clearDataMaps(absoluteTableIdentifier)
    }
  }

  private def getTimestampFileAndType() = {
    var basePath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT)
    basePath = CarbonUtil.checkAndAppendFileSystemURIScheme(basePath)
    val timestampFile = basePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    val timestampFileType = FileFactory.getFileType(timestampFile)
    if (!FileFactory.isFileExist(basePath, timestampFileType)) {
      FileFactory.mkdirs(basePath, timestampFileType)
    }
    (timestampFile, timestampFileType)
  }

  /**
   * This method will put the updated timestamp of schema file in the table modified time store map
   *
   * @param timeStamp
   */
  private def updateSchemasUpdatedTime(timeStamp: Long) {
    tableModifiedTimeStore.put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, timeStamp)
  }

  def updateAndTouchSchemasUpdatedTime() {
    updateSchemasUpdatedTime(touchSchemaFileSystemTime())
  }


  /**
   * This method will check and create an empty schema timestamp file
   *
   * @return
   */
  private def touchSchemaFileSystemTime(): Long = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType()
    if (!FileFactory.isFileExist(timestampFile, timestampFileType)) {
      LOGGER.audit(s"Creating timestamp file for $timestampFile")
      FileFactory.createNewFile(timestampFile, timestampFileType)
    }
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(System.currentTimeMillis())
    // since there is no guarantee that exact same set modified time returns when called
    // lastmodified time, so better get the time from file.
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .getLastModifiedTime
  }

  def checkSchemasModifiedTimeAndReloadTables() {
    val (timestampFile, timestampFileType) = getTimestampFileAndType()
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      if (!(FileFactory.getCarbonFile(timestampFile, timestampFileType).
        getLastModifiedTime ==
            tableModifiedTimeStore.get(CarbonCommonConstants.DATABASE_DEFAULT_NAME))) {
        refreshCache()
      }
    }
  }

  private def refreshCache() {
    metadata.carbonTables.clear()
  }

  override def isReadFromHiveMetaStore: Boolean = false

  override def listAllTables(sparkSession: SparkSession): Seq[CarbonTable] =
    metadata.carbonTables

  override def getThriftTableInfo(tablePath: CarbonTablePath)
    (sparkSession: SparkSession): TableInfo = {
    val tableMetadataFile = tablePath.getSchemaFilePath
    CarbonUtil.readSchemaFile(tableMetadataFile)
  }
}
