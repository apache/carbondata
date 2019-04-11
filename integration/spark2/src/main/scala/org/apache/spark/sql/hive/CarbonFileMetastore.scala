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

import java.io.IOException
import java.net.URI

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.CarbonExpressions.{CarbonSubqueryAlias => SubqueryAlias}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.FileWriteOperation
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema
import org.apache.carbondata.core.metadata.schema.{table, SchemaReader}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
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
    dictionaryMap: DictionaryMap,
    hasAggregateDataMapSchema: Boolean)

case class DictionaryMap(dictionaryMap: Map[String, Boolean]) {
  def get(name: String): Option[Boolean] = {
    dictionaryMap.get(name.toLowerCase)
  }
}

object MatchLogicalRelation {
  def unapply(logicalPlan: LogicalPlan): Option[(BaseRelation, Any, Any)] = logicalPlan match {
    case l: LogicalRelation => Some(l.relation, l.output, l.catalogTable)
    case _ => None
  }
}

class CarbonFileMetastore extends CarbonMetaStore {

  @transient
  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.CarbonMetastoreCatalog")

  val tableModifiedTimeStore = new java.util.HashMap[String, Long]()
  tableModifiedTimeStore
    .put(CarbonCommonConstants.DATABASE_DEFAULT_NAME, System.currentTimeMillis())

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
        readCarbonSchema(absIdentifier,
          !parameters.getOrElse("isTransactional", "true").toBoolean) match {
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
      MatchLogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _)) =>
        carbonDatasourceHadoopRelation.carbonRelation
      case MatchLogicalRelation(
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        carbonDatasourceHadoopRelation.carbonRelation
      case SubqueryAlias(_, c)
        if (SparkUtil.isSparkVersionXandAbove("2.2")) &&
           (c.getClass.getName.equals("org.apache.spark.sql.catalyst.catalog.CatalogRelation") ||
            c.getClass.getName.equals("org.apache.spark.sql.catalyst.catalog.HiveTableRelation") ||
            c.getClass.getName.equals(
              "org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation")) =>
        val catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable("tableMeta", c).asInstanceOf[CatalogTable]
        catalogTable.provider match {
          case Some(name) if (name.equals("org.apache.spark.sql.CarbonSource")
            || name.equalsIgnoreCase("carbondata")) => name
          case _ => throw new NoSuchTableException(database, tableIdentifier.table)
        }
        val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
           catalogTable.location.toString, database, tableIdentifier.table)
        CarbonEnv.getInstance(sparkSession).carbonMetaStore.
          createCarbonRelation(catalogTable.storage.properties, identifier, sparkSession)
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
      case _: Exception =>
        return false
    }
    true
  }

  def isTableInMetastore(identifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): Boolean = {
    sparkSession.sessionState.catalog.listTables(identifier.getDatabaseName)
      .exists(_.table.equalsIgnoreCase(identifier.getTableName))
  }


  private def readCarbonSchema(identifier: AbsoluteTableIdentifier,
      inferSchema: Boolean): Option[CarbonTable] = {

    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val dbName = identifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = identifier.getCarbonTableIdentifier.getTableName
    val tableUniqueName = CarbonTable.buildUniqueName(dbName, tableName)
    val tablePath = identifier.getTablePath
    val wrapperTableInfo =
    if (inferSchema) {
      val carbonTbl = CarbonMetadata.getInstance().getCarbonTable(dbName, tableName)
      val tblInfoFromCache = if (carbonTbl != null) {
        carbonTbl.getTableInfo
      } else {
        null
      }

      val thriftTableInfo : TableInfo = if (tblInfoFromCache != null) {
        // In case the TableInfo is present in the Carbon Metadata Cache
        // then get the tableinfo from the cache rather than infering from
        // the CarbonData file.
        schemaConverter
          .fromWrapperToExternalTableInfo(tblInfoFromCache, dbName, tableName)
      } else {
        schemaConverter
          .fromWrapperToExternalTableInfo(SchemaReader
                      .inferSchema(identifier, false),
            dbName, tableName)
      }

      val wrapperTableInfo =
        schemaConverter
          .fromExternalToWrapperTableInfo(thriftTableInfo, dbName, tableName, tablePath)
      wrapperTableInfo.getFactTable.getTableProperties.put("_external", "true")
      wrapperTableInfo.setTransactionalTable(false)
      Some(wrapperTableInfo)
    } else {
      val tableMetadataFile = CarbonTablePath.getSchemaFilePath(tablePath)
      val fileType = FileFactory.getFileType(tableMetadataFile)
      if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
        val tableInfo: TableInfo = CarbonUtil.readSchemaFile(tableMetadataFile)
        val wrapperTableInfo =
          schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath)
        Some(wrapperTableInfo)
      } else {
        None
      }
    }

    wrapperTableInfo.map { tableInfo =>
      CarbonMetadata.getInstance().removeTable(tableUniqueName)
      CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
      val carbonTable = CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
      metadata.carbonTables += carbonTable
      carbonTable
    }
  }

  /**
   * This method will overwrite the existing schema and update it with the given details
   */
  def updateTableSchemaForAlter(
      newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      tablePath: String) (sparkSession: SparkSession): String = {
    val identifier = AbsoluteTableIdentifier.from(tablePath, oldTableIdentifier)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    if (schemaEvolutionEntry != null) {
      thriftTableInfo.fact_table.schema_evolution.schema_evolution_history.add(schemaEvolutionEntry)
    }
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
      thriftTableInfo,
      newTableIdentifier.getDatabaseName,
      newTableIdentifier.getTableName,
      identifier.getTablePath)
    val newAbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
      identifier.getTablePath,
      newTableIdentifier.getDatabaseName,
      newTableIdentifier.getTableName,
      oldTableIdentifier.getTableId)
    val path = createSchemaThriftFile(newAbsoluteTableIdentifier, thriftTableInfo)
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
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
      thriftTableInfo,
      carbonTableIdentifier.getDatabaseName,
      carbonTableIdentifier.getTableName,
      absoluteTableIdentifier.getTablePath)
    val evolutionEntries = thriftTableInfo.fact_table.schema_evolution.schema_evolution_history
    evolutionEntries.remove(evolutionEntries.size() - 1)
    val path = createSchemaThriftFile(absoluteTableIdentifier, thriftTableInfo)
    addTableCache(wrapperTableInfo, absoluteTableIdentifier)
    path
  }

  override def revertTableSchemaForPreAggCreationFailure(
      absoluteTableIdentifier: AbsoluteTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
      thriftTableInfo,
      absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName,
      absoluteTableIdentifier.getCarbonTableIdentifier.getTableName,
      absoluteTableIdentifier.getTablePath)
    val childSchemaList = wrapperTableInfo.getDataMapSchemaList
    childSchemaList.remove(childSchemaList.size() - 1)
    val path = createSchemaThriftFile(absoluteTableIdentifier, thriftTableInfo)
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
    val thriftTableInfo = schemaConverter.fromWrapperToExternalTableInfo(
      tableInfo, dbName, tableName)
    val identifier = AbsoluteTableIdentifier
      .from(tablePath, dbName, tableName, thriftTableInfo.getFact_table.getTable_id)
    createSchemaThriftFile(identifier, thriftTableInfo)
    LOGGER.info(s"Table $tableName for Database $dbName created successfully.")
  }

  /**
   * Generates schema string from TableInfo
   */
  override def generateTableSchemaString(
      tableInfo: schema.table.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): String = {
    val schemaEvolutionEntry = new schema.SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvolution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    removeTableFromMetadata(tableInfo.getDatabaseName, tableInfo.getFactTable.getTableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
    addTableCache(tableInfo, absoluteTableIdentifier)
    CarbonUtil.convertToMultiGsonStrings(tableInfo, " ", "", ",")
  }

  /**
   * This method will write the schema thrift file in carbon store and load table metadata
   */
  private def createSchemaThriftFile(
      identifier: AbsoluteTableIdentifier,
      thriftTableInfo: TableInfo): String = {
    val schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
    val schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath)
    val fileType = FileFactory.getFileType(schemaMetadataPath)
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      val isDirCreated = FileFactory
        .mkdirs(schemaMetadataPath, SparkSession.getActiveSession.get.sessionState.newHadoopConf())
      if (!isDirCreated) {
        throw new IOException(s"Failed to create the metadata directory $schemaMetadataPath")
      }
    }
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open(FileWriteOperation.OVERWRITE)
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    updateSchemasUpdatedTime(touchSchemaFileSystemTime())
    identifier.getTablePath
  }

  protected def addTableCache(
      tableInfo: table.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): ArrayBuffer[CarbonTable] = {
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
      case None =>
        if (LOGGER.isDebugEnabled) {
          LOGGER.debug(s"No entry for table $tableName in database $dbName")
        }
    }
    CarbonMetadata.getInstance.removeTable(dbName, tableName)
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
    val wrapperTableInfo =
      schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath)
    updateMetadataByWrapperTable(wrapperTableInfo)
  }


  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    try {
      val tablePath = lookupRelation(tableIdentifier)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable.getTablePath
      val fileType = FileFactory.getFileType(tablePath)
      FileFactory.isFileExist(tablePath, fileType)
    } catch {
      case _: Exception =>
       false
    }
  }


  def dropTable(absoluteTableIdentifier: AbsoluteTableIdentifier)
    (sparkSession: SparkSession) {
    val dbName = absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = absoluteTableIdentifier.getCarbonTableIdentifier.getTableName
    val metadataFilePath = CarbonTablePath.getMetadataPath(absoluteTableIdentifier.getTablePath)
    val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName, tableName)
    if (null != carbonTable) {
      // clear driver B-tree and dictionary cache
      ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(carbonTable)
    }
    val fileType = FileFactory.getFileType(metadataFilePath)

    if (FileFactory.isFileExist(metadataFilePath, fileType)) {
      // while drop we should refresh the schema modified time so that if any thing has changed
      // in the other beeline need to update.
      checkSchemasModifiedTimeAndReloadTable(TableIdentifier(tableName, Some(dbName)))

      CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
      updateSchemasUpdatedTime(touchSchemaFileSystemTime())
      // discard cached table info in cachedDataSourceTables
      val tableIdentifier = TableIdentifier(tableName, Option(dbName))
      sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
      DataMapStoreManager.getInstance().clearDataMaps(absoluteTableIdentifier)
      SegmentPropertiesAndSchemaHolder.getInstance().invalidate(absoluteTableIdentifier)
      removeTableFromMetadata(dbName, tableName)
    } else {
      if (!isTransactionalCarbonTable(absoluteTableIdentifier)) {
        removeTableFromMetadata(dbName, tableName)
        CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
        // discard cached table info in cachedDataSourceTables
        val tableIdentifier = TableIdentifier(tableName, Option(dbName))
        sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
        DataMapStoreManager.getInstance().clearDataMaps(absoluteTableIdentifier)
        SegmentPropertiesAndSchemaHolder.getInstance().invalidate(absoluteTableIdentifier)
        removeTableFromMetadata(dbName, tableName)
      }
    }
  }


  def isTransactionalCarbonTable(identifier: AbsoluteTableIdentifier): Boolean = {
    val table = getTableFromMetadataCache(identifier.getDatabaseName, identifier.getTableName)
    table.map(_.getTableInfo.isTransactionalTable).getOrElse(true)
  }

  private def getTimestampFileAndType() = {
    var basePath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
        CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT)
    basePath = CarbonUtil.checkAndAppendFileSystemURIScheme(basePath)
    val timestampFile = basePath + "/" + CarbonCommonConstants.SCHEMAS_MODIFIED_TIME_FILE
    val timestampFileType = FileFactory.getFileType(timestampFile)
    if (!FileFactory.isFileExist(basePath, timestampFileType)) {
      FileFactory
        .createDirectoryAndSetPermission(basePath,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
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
      LOGGER.info(s"Creating timestamp file for $timestampFile")
      FileFactory
        .createNewFile(timestampFile,
          timestampFileType,
          true,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    }
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .setLastModifiedTime(System.currentTimeMillis())
    // since there is no guarantee that exact same set modified time returns when called
    // lastmodified time, so better get the time from file.
    FileFactory.getCarbonFile(timestampFile, timestampFileType)
      .getLastModifiedTime
  }

  def checkSchemasModifiedTimeAndReloadTable(tableIdentifier: TableIdentifier): Boolean = {
    val (timestampFile, timestampFileType) = getTimestampFileAndType()
    var isRefreshed = false
    if (FileFactory.isFileExist(timestampFile, timestampFileType)) {
      val lastModifiedTime =
        FileFactory.getCarbonFile(timestampFile, timestampFileType).getLastModifiedTime
      if (!(lastModifiedTime ==
            tableModifiedTimeStore.get(CarbonCommonConstants.DATABASE_DEFAULT_NAME))) {
        metadata.carbonTables = metadata.carbonTables.filterNot(
          table => table.getTableName.equalsIgnoreCase(tableIdentifier.table) &&
                   table.getDatabaseName
                     .equalsIgnoreCase(tableIdentifier.database
                       .getOrElse(SparkSession.getActiveSession.get.sessionState.catalog
                         .getCurrentDatabase)))
        updateSchemasUpdatedTime(lastModifiedTime)
        isRefreshed = true
      }
    }
    isRefreshed
  }

  override def isReadFromHiveMetaStore: Boolean = false

  override def listAllTables(sparkSession: SparkSession): Seq[CarbonTable] =
    metadata.carbonTables

  override def getThriftTableInfo(carbonTable: CarbonTable): TableInfo = {
    val tableMetadataFile = CarbonTablePath.getSchemaFilePath(carbonTable.getTablePath)
    CarbonUtil.readSchemaFile(tableMetadataFile)
  }

  override def createCarbonDataSourceHadoopRelation(
      sparkSession: SparkSession,
      tableIdentifier: TableIdentifier): CarbonDatasourceHadoopRelation = {
    val relation: LogicalPlan = sparkSession.sessionState.catalog.lookupRelation(tableIdentifier)
    relation match {
      case SubqueryAlias(_,
      MatchLogicalRelation(carbonDataSourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _)) =>
        carbonDataSourceHadoopRelation
      case MatchLogicalRelation(
      carbonDataSourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        carbonDataSourceHadoopRelation
      case SubqueryAlias(_, c)
        if (SparkUtil.isSparkVersionXandAbove("2.2")) &&
           (c.getClass.getName.equals("org.apache.spark.sql.catalyst.catalog.CatalogRelation") ||
            c.getClass.getName
              .equals("org.apache.spark.sql.catalyst.catalog.HiveTableRelation") ||
            c.getClass.getName.equals(
              "org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation")) =>
        val catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable("tableMeta", c).asInstanceOf[CatalogTable]
        catalogTable.provider match {
          case Some(name) if (name.equals("org.apache.spark.sql.CarbonSource")
            || name.equalsIgnoreCase("carbondata")) => name
          case _ =>
            throw new NoSuchTableException(tableIdentifier.database.get, tableIdentifier.table)
        }
        val tableLocation = catalogTable.storage.locationUri match {
          case tableLoc@Some(uri) =>
            if (tableLoc.get.isInstanceOf[URI]) {
              FileFactory.getUpdatedFilePath(tableLoc.get.asInstanceOf[URI].getPath)
            }
          case None =>
            CarbonEnv.getTablePath(tableIdentifier.database, tableIdentifier.table)(sparkSession)
        }
        CarbonDatasourceHadoopRelation(sparkSession,
          Array(tableLocation.asInstanceOf[String]),
          catalogTable.storage.properties,
          Option(catalogTable.schema))
      case _ => throw new NoSuchTableException(tableIdentifier.database.get, tableIdentifier.table)
    }
  }
}
