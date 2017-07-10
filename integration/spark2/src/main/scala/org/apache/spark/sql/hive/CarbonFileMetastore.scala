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
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.carbondata.core.fileoperations.FileWriteOperation
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
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

  lazy val metadata = loadMetadata(storePath, nextQueryId)


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
      Some(absIdentifier.getCarbonTableIdentifier.getDatabaseName)), true)(sparkSession)
      .asInstanceOf[CarbonRelation]
  }

  def lookupRelation(dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): LogicalPlan = {
    lookupRelation(TableIdentifier(tableName, dbName))(sparkSession)
  }

  def lookupRelation(tableIdentifier: TableIdentifier, readFromStore: Boolean = false)
    (sparkSession: SparkSession): LogicalPlan = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(
      sparkSession.catalog.currentDatabase
    )
    val tables = getTableFromMetadata(database, tableIdentifier.table, true)
    tables match {
      case Some(t) =>
        CarbonRelation(database, tableIdentifier.table,
          CarbonSparkUtil.createSparkMeta(tables.head.carbonTable), tables.head)
      case None =>
        throw new NoSuchTableException(database, tableIdentifier.table)
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

  def tableExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    checkSchemasModifiedTimeAndReloadTables()
    val database = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val tables = metadata.tablesMeta.filter(
      c => c.carbonTableIdentifier.getDatabaseName.equalsIgnoreCase(database) &&
           c.carbonTableIdentifier.getTableName.equalsIgnoreCase(tableIdentifier.table))
    tables.nonEmpty
  }

  def loadMetadata(metadataPath: String, queryId: String): MetaData = {
    val recorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val statistic = new QueryStatistic()
    // creating zookeeper instance once.
    // if zookeeper is configured as carbon lock type.
    val zookeeperurl = conf.get(CarbonCommonConstants.ZOOKEEPER_URL, null)
    if (null != zookeeperurl) {
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.ZOOKEEPER_URL, zookeeperurl)
    }
    if (metadataPath == null) {
      return null
    }
    // if no locktype is configured and store type is HDFS set HDFS lock as default
    if (null == CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.LOCK_TYPE) &&
        FileType.HDFS == FileFactory.getFileType(metadataPath)) {
      CarbonProperties.getInstance
        .addProperty(CarbonCommonConstants.LOCK_TYPE,
          CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS
        )
      LOGGER.info("Default lock type HDFSLOCK is configured")
    }
    val fileType = FileFactory.getFileType(metadataPath)
    val metaDataBuffer = new ArrayBuffer[TableMeta]
    fillMetaData(metadataPath, fileType, metaDataBuffer)
    updateSchemasUpdatedTime(readSchemaFileSystemTime("", ""))
    statistic.addStatistics(QueryStatisticsConstants.LOAD_META,
      System.currentTimeMillis())
    recorder.recordStatisticsForDriver(statistic, queryId)
    MetaData(metaDataBuffer)
  }

  private def fillMetaData(basePath: String, fileType: FileType,
      metaDataBuffer: ArrayBuffer[TableMeta]): Unit = {
    val databasePath = basePath // + "/schemas"
    try {
      if (FileFactory.isFileExist(databasePath, fileType)) {
        val file = FileFactory.getCarbonFile(databasePath, fileType)
        val databaseFolders = file.listFiles()

        databaseFolders.foreach(databaseFolder => {
          if (databaseFolder.isDirectory) {
            val dbName = databaseFolder.getName
            val tableFolders = databaseFolder.listFiles()

            tableFolders.foreach(tableFolder => {
              if (tableFolder.isDirectory) {
                val carbonTableIdentifier = new CarbonTableIdentifier(databaseFolder.getName,
                  tableFolder.getName, UUID.randomUUID().toString)
                val carbonTablePath = CarbonStorePath.getCarbonTablePath(basePath,
                  carbonTableIdentifier)
                val tableMetadataFile = carbonTablePath.getSchemaFilePath

                if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
                  val tableName = tableFolder.getName
                  val tableUniqueName = databaseFolder.getName + "_" + tableFolder.getName
                  val tableInfo: TableInfo = CarbonUtil.readSchemaFile(tableMetadataFile)
                  val schemaConverter = new ThriftWrapperSchemaConverterImpl
                  val wrapperTableInfo = schemaConverter
                    .fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, basePath)
                  val schemaFilePath = CarbonStorePath
                    .getCarbonTablePath(storePath, carbonTableIdentifier).getSchemaFilePath
                  wrapperTableInfo.setStorePath(storePath)
                  wrapperTableInfo
                    .setMetaDataFilepath(CarbonTablePath.getFolderContainingFile(schemaFilePath))
                  CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
                  val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
                  metaDataBuffer += new TableMeta(carbonTable.getCarbonTableIdentifier, storePath,
                    carbonTable)
                }
              }
            })
          }
        })
      } else {
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)
      }
    } catch {
      case s: java.io.FileNotFoundException =>
        s.printStackTrace()
        // Create folders and files.
        FileFactory.mkdirs(databasePath, fileType)
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
    thriftTableInfo.fact_table.schema_evolution.schema_evolution_history.add(schemaEvolutionEntry)
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
          newTableIdentifier.getDatabaseName,
          newTableIdentifier.getTableName,
          carbonStorePath)
    createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      newTableIdentifier.getDatabaseName,
      newTableIdentifier.getTableName)(sparkSession)
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
    createSchemaThriftFile(wrapperTableInfo,
      thriftTableInfo,
      carbonTableIdentifier.getDatabaseName,
      carbonTableIdentifier.getTableName)(sparkSession)
  }



  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableInfo
   *
   */
  def createTableFromThrift(
      tableInfo: org.apache.carbondata.core.metadata.schema.table.TableInfo,
      dbName: String, tableName: String)(sparkSession: SparkSession): (String, String) = {
    if (tableExists(tableName, Some(dbName))(sparkSession)) {
      sys.error(s"Table [$tableName] already exists under Database [$dbName]")
    }
    val schemaEvolutionEntry = new SchemaEvolutionEntry(tableInfo.getLastUpdatedTime)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val thriftTableInfo = schemaConverter
      .fromWrapperToExternalTableInfo(tableInfo, dbName, tableName)
    thriftTableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history
      .add(schemaEvolutionEntry)
    val carbonTablePath = createSchemaThriftFile(tableInfo,
      thriftTableInfo,
      dbName,
      tableName)(sparkSession)
    LOGGER.info(s"Table $tableName for Database $dbName created successfully.")
    (carbonTablePath, "")
  }

  /**
   * This method will write the schema thrift file in carbon store and load table metadata
   *
   * @param tableInfo
   * @param thriftTableInfo
   * @param dbName
   * @param tableName
   * @param sparkSession
   * @return
   */
  private def createSchemaThriftFile(
      tableInfo: org.apache.carbondata.core.metadata.schema.table.TableInfo,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      dbName: String, tableName: String)
    (sparkSession: SparkSession): String = {
    val carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableName,
      tableInfo.getFactTable.getTableId)
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
    removeTableFromMetadata(dbName, tableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    val tableMeta = new TableMeta(carbonTableIdentifier, storePath,
      CarbonMetadata.getInstance().getCarbonTable(dbName + '_' + tableName))
    metadata.tablesMeta += tableMeta
    updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
    carbonTablePath.getPath
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
        LOGGER.debug(s"No entry for table $tableName in database $dbName")
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
      val file = FileFactory.getCarbonFile(metadataFilePath, fileType)
      CarbonUtil.deleteFoldersAndFilesSilent(file.getParentFile)
      val metadataToBeRemoved: Option[TableMeta] = getTableFromMetadata(dbName,
        tableIdentifier.table)
      metadataToBeRemoved match {
        case Some(tableMeta) =>
          metadata.tablesMeta -= tableMeta
          CarbonMetadata.getInstance.removeTable(dbName + "_" + tableName)
          updateSchemasUpdatedTime(touchSchemaFileSystemTime(dbName, tableName))
        case None =>
          LOGGER.info(s"Metadata does not contain entry for table $tableName in database $dbName")
      }
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
    metadata.tablesMeta = loadMetadata(storePath, nextQueryId).tablesMeta
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

