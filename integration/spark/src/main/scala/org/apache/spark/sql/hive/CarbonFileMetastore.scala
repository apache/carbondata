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
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import net.jodah.expiringmap.{ExpirationPolicy, ExpiringMap}
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, CarbonSource, EnvHelper, SparkSession}
import org.apache.spark.sql.CarbonExpressions.{CarbonSubqueryAlias => SubqueryAlias}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.FileWriteOperation
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.table
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.ThriftWriter
import org.apache.carbondata.events.{CreateCarbonRelationPostEvent, LookupRelationPostEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}

object MatchLogicalRelation {
  def unapply(
      logicalPlan: LogicalPlan
  ): Option[(BaseRelation, Any, Option[CatalogTable])] = logicalPlan match {
    case l: LogicalRelation => Some(l.relation, l.output, l.catalogTable)
    case _ => None
  }
}

private object CarbonFileMetastore {

  final val tableModifiedTimeStore = ExpiringMap.builder()
    .expiration(CarbonProperties.getInstance().getMetaCacheExpirationTime, TimeUnit.SECONDS)
    .expirationPolicy(ExpirationPolicy.ACCESSED).build[String, java.lang.Long]

  def checkIfRefreshIsNeeded(absoluteTableIdentifier: AbsoluteTableIdentifier,
      localTimeStamp: Long): Boolean = {
    val schemaFilePath = CarbonTablePath.getSchemaFilePath(absoluteTableIdentifier.getTablePath)
    val schemaCarbonFile = FileFactory.getCarbonFile(schemaFilePath)
    if (schemaCarbonFile.exists()) {
      val oldTime = Option(CarbonFileMetastore.getTableModifiedTime(absoluteTableIdentifier
        .getCarbonTableIdentifier
        .getTableId))
      val newTime = schemaCarbonFile.getLastModifiedTime
      val isSchemaModified = oldTime match {
        case Some(cacheTime) =>
          cacheTime != newTime
        case None => true
      }
      if (isSchemaModified) {
        if (CarbonMetadata.getInstance()
              .getCarbonTable(absoluteTableIdentifier
                .getCarbonTableIdentifier
                .getTableUniqueName) != null) {
          synchronized {
            if (CarbonMetadata.getInstance()
                  .getCarbonTable(absoluteTableIdentifier
                    .getCarbonTableIdentifier
                    .getTableUniqueName) != null) {
              CarbonMetadata.getInstance().removeTable(absoluteTableIdentifier
                .getCarbonTableIdentifier.getTableUniqueName)
              IndexStoreManager.getInstance().clearIndex(absoluteTableIdentifier)
            }
          }
        }
        true
      } else {
        localTimeStamp != newTime
      }
    } else {
      true
    }
  }

  def updateTableSchemaModifiedTime(tableUniqueId: String, timeStamp: Long): Unit = {
    tableModifiedTimeStore.put(tableUniqueId, timeStamp)
  }

  def getTableModifiedTime(tableUniqueId: String): java.lang.Long = {
    tableModifiedTimeStore.get(tableUniqueId)
  }

  def removeStaleEntries(invalidTableUniqueIds: List[String]) {
    for (invalidKey <- invalidTableUniqueIds) {
      tableModifiedTimeStore.remove(invalidKey)
    }
  }
}

class CarbonFileMetastore extends CarbonMetaStore {

  @transient private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Create Carbon Relation by reading the schema file
   */
  override def createCarbonRelation(parameters: Map[String, String],
      absIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): CarbonRelation = {
    val database = absIdentifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = absIdentifier.getCarbonTableIdentifier.getTableName
    val tables = Option(CarbonMetadata.getInstance.getCarbonTable(database, tableName))
    tables match {
      case Some(t) =>
        if (t.getTablePath.equals(absIdentifier.getTablePath)) {
          if (isSchemaRefreshed(t.getAbsoluteTableIdentifier, sparkSession)) {
            readCarbonSchema(t.getAbsoluteTableIdentifier, parameters, sparkSession, false)
          } else {
            CarbonRelation(database, tableName, t)
          }
        } else {
          IndexStoreManager.getInstance().clearIndex(absIdentifier)
          CarbonMetadata.getInstance().removeTable(
            absIdentifier.getCarbonTableIdentifier.getTableUniqueName)
          readCarbonSchema(absIdentifier, parameters, sparkSession)
        }
      case None =>
        readCarbonSchema(absIdentifier, parameters, sparkSession)
    }
  }

  private def readCarbonSchema(absIdentifier: AbsoluteTableIdentifier,
      parameters: Map[String, String],
      sparkSession: SparkSession,
      needLock: Boolean = true): CarbonRelation = {
    val relation = readCarbonSchema(absIdentifier, parameters,
      !parameters.getOrElse("isTransactional", "true").toBoolean) match {
      case Some(meta) =>
        CarbonRelation(absIdentifier.getDatabaseName, absIdentifier.getTableName, meta)
      case None =>
        throw new NoSuchTableException(absIdentifier.getDatabaseName, absIdentifier.getTableName)
    }
    // fire post event after lookup relation
    val operationContext = new OperationContext
    val createCarbonRelationPostEvent: CreateCarbonRelationPostEvent =
      CreateCarbonRelationPostEvent(
        sparkSession, relation.carbonTable, needLock)
    OperationListenerBus.getInstance.fireEvent(createCarbonRelationPostEvent, operationContext)
    relation
  }

  /**
   * This method will overwrite the existing schema and update it with the given details.
   */
  def updateTableSchema(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      carbonStorePath: String)(sparkSession: SparkSession): String = {
    updateTableSchemaForAlter(newTableIdentifier,
      oldTableIdentifier, thriftTableInfo, null, carbonStorePath) (sparkSession)
  }

  def lookupRelation(dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): CarbonRelation = {
    lookupRelation(TableIdentifier(tableName, dbName))(sparkSession)
  }

  override def lookupRelation(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): CarbonRelation = {
    val database = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val relation = sparkSession.sessionState.catalog.lookupRelation(tableIdentifier) match {
      case SubqueryAlias(_,
      MatchLogicalRelation(carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _)) =>
        carbonDatasourceHadoopRelation.carbonRelation
      case MatchLogicalRelation(
      carbonDatasourceHadoopRelation: CarbonDatasourceHadoopRelation, _, _) =>
        carbonDatasourceHadoopRelation.carbonRelation
      case SubqueryAlias(_, c) if SparkSQLUtil.isRelation(c.getClass.getName) =>
        val catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable("tableMeta", c).asInstanceOf[CatalogTable]
        if (!CarbonSource.isCarbonDataSource(catalogTable)) {
          CarbonMetadata.getInstance().removeTable(database, tableIdentifier.table)
          throw new NoSuchTableException(database, tableIdentifier.table)
        }
        val identifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
           catalogTable.location.toString, database, tableIdentifier.table)
        CarbonEnv.getInstance(sparkSession).carbonMetaStore.
          createCarbonRelation(catalogTable.storage.properties, identifier, sparkSession)
      case _ =>
        CarbonMetadata.getInstance().removeTable(database, tableIdentifier.table)
        throw new NoSuchTableException(database, tableIdentifier.table)
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

  override def lookupAnyRelation(
      dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): LogicalPlan = {
    val tableIdentifier = new TableIdentifier(tableName, dbName)
    val rawRelation = sparkSession.sessionState.catalog.lookupRelation(tableIdentifier)
    rawRelation match {
      case SubqueryAlias(_, c) if SparkSQLUtil.isRelation(c.getClass.getName) =>
        var catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable("tableMeta", c).asInstanceOf[CatalogTable]
        // Here, catalogTable will have spatial column in schema which is used to build carbon
        // table. As spatial column is not supposed to be present in user-defined columns,
        // removing it here. Later from tableproperties the column will be added in carbonTable.
        val spatialProperty = catalogTable.storage.properties
          .get(CarbonCommonConstants.SPATIAL_INDEX)
        if (spatialProperty.isDefined) {
          val originalSchema = StructType(catalogTable.schema.
            filterNot(_.name.equalsIgnoreCase(spatialProperty.get.trim)))
          catalogTable = catalogTable.copy(schema = originalSchema)
        }
        val tableInfo = CarbonSparkSqlParserUtil.buildTableInfoFromCatalogTable(
          catalogTable, false, sparkSession)
        val carbonTable = CarbonTable.buildFromTableInfo(tableInfo)
        CarbonRelation(carbonTable.getDatabaseName, carbonTable.getTableName, carbonTable)
      case _ =>
        throw new NoSuchTableException(
          sparkSession.sessionState.catalog.getCurrentDatabase, tableIdentifier.table)
    }
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
      case _: NoSuchTableException =>
        LOGGER.debug(s"Table ${tableIdentifier.table} does not exist.")
        return false
      case ex: Exception =>
        throw ex
    }
    true
  }

  private def readCarbonSchema(identifier: AbsoluteTableIdentifier,
      parameters: Map[String, String], inferSchema: Boolean): Option[CarbonTable] = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val dbName = identifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = identifier.getCarbonTableIdentifier.getTableName
    val tableUniqueName = CarbonTable.buildUniqueName(dbName, tableName)
    val tablePath = identifier.getTablePath
    var schemaRefreshTime = System.currentTimeMillis()
    val wrapperTableInfo =
      if (inferSchema) {
        val carbonTbl = CarbonMetadata.getInstance().getCarbonTable(dbName, tableName)
        val tblInfoFromCache = if (carbonTbl != null) {
          carbonTbl.getTableInfo
        } else {
          CarbonUtil.convertGsonToTableInfo(parameters.asJava)
        }

        val thriftTableInfo: TableInfo = if (tblInfoFromCache != null) {
          // In case the TableInfo is present in the Carbon Metadata Cache
          // then get the table info from the cache rather than inferring from
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
        if (FileFactory.isFileExist(tableMetadataFile)) {
          val tableInfo: TableInfo = CarbonUtil.readSchemaFile(tableMetadataFile)
          val wrapperTableInfo =
            schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath)
          schemaRefreshTime = FileFactory
            .getCarbonFile(tableMetadataFile).getLastModifiedTime
          // set external property to table info from catalog table properties
          if (parameters.contains("isExternal")) {
            wrapperTableInfo.getFactTable
              .getTableProperties
              .put("_external", parameters("isExternal"))
          }
          if (parameters.contains("latestversion")) {
            wrapperTableInfo.getFactTable
              .getTableProperties
              .put("latestversion", parameters("latestversion"))
          }
          Some(wrapperTableInfo)
        } else {
          None
        }
      }
    wrapperTableInfo.map { tableInfo =>
      updateSchemasUpdatedTime(tableInfo.getFactTable.getTableId, schemaRefreshTime)
      CarbonMetadata.getInstance().removeTable(tableUniqueName)
      CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
      CarbonMetadata.getInstance().getCarbonTable(tableUniqueName)
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
    addCarbonTableToCache(wrapperTableInfo, newAbsoluteTableIdentifier)
    path
  }

  /**
   * This method will is used to remove the evolution entry in case of failure.
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
    addCarbonTableToCache(wrapperTableInfo, absoluteTableIdentifier)
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
    addCarbonTableToCache(tableInfo, absoluteTableIdentifier)
    CarbonUtil.convertToMultiGsonStrings(tableInfo, " ", "", ",")
  }

  /**
   * This method will write the schema thrift file in carbon store and load table metadata
   */
  private def createSchemaThriftFile(
      identifier: AbsoluteTableIdentifier,
      thriftTableInfo: TableInfo): String = {
    val schemaMetadataPath = CarbonTablePath.getMetadataPath(identifier.getTablePath)
    if (!FileFactory.isFileExist(schemaMetadataPath)) {
      val isDirCreated = FileFactory
        .mkdirs(schemaMetadataPath, SparkSession.getActiveSession.get.sessionState.newHadoopConf())
      if (!isDirCreated) {
        throw new IOException(s"Failed to create the metadata directory $schemaMetadataPath")
      }
    }
    val schemaFilePath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath)
    val thriftWriter = new ThriftWriter(schemaFilePath, false)
    thriftWriter.open(FileWriteOperation.OVERWRITE)
    thriftWriter.write(thriftTableInfo)
    thriftWriter.close()
    val modifiedTime = System.currentTimeMillis()
    FileFactory.getCarbonFile(schemaFilePath).setLastModifiedTime(modifiedTime)
    updateSchemasUpdatedTime(identifier.getCarbonTableIdentifier.getTableId, modifiedTime)
    identifier.getTablePath
  }

  protected def addCarbonTableToCache(
      tableInfo: table.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): Unit = {
    val identifier = absoluteTableIdentifier.getCarbonTableIdentifier
    removeTableFromMetadata(identifier.getDatabaseName, identifier.getTableName)
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo)
  }

  /**
   * This method will remove the table meta from CarbonMetadata cache.
   */
  def removeTableFromMetadata(dbName: String, tableName: String): Unit = {
    CarbonMetadata.getInstance.removeTable(dbName, tableName)
  }

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, tablePath: String): Unit = {
    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis())
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val wrapperTableInfo =
      schemaConverter.fromExternalToWrapperTableInfo(tableInfo, dbName, tableName, tablePath)
    addCarbonTableToCache(wrapperTableInfo,
      wrapperTableInfo.getOrCreateAbsoluteTableIdentifier())
  }


  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean = {
    try {
      val tablePath = lookupRelation(tableIdentifier)(sparkSession)
        .asInstanceOf[CarbonRelation].carbonTable.getTablePath
      FileFactory.isFileExist(tablePath)
    } catch {
      case _: Exception =>
       false
    }
  }

  def dropTable(absoluteTableIdentifier: AbsoluteTableIdentifier)(sparkSession: SparkSession) {
    val dbName = absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = absoluteTableIdentifier.getCarbonTableIdentifier.getTableName
    // Clear both driver and executor cache.
    IndexStoreManager.getInstance().clearIndex(absoluteTableIdentifier)
    CarbonHiveIndexMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
    // discard cached table info in cachedDataSourceTables
    val tableIdentifier = TableIdentifier(tableName, Option(dbName))
    sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
    SegmentPropertiesAndSchemaHolder.getInstance().invalidate(absoluteTableIdentifier)
    removeTableFromMetadata(dbName, tableName)
  }

  def isTransactionalCarbonTable(identifier: AbsoluteTableIdentifier): Boolean = {
    val table = Option(CarbonMetadata.getInstance()
      .getCarbonTable(identifier.getCarbonTableIdentifier.getTableUniqueName))
    table match {
      case Some(t) => t.isTransactionalTable
      case None => true
    }
  }

  /**
   * This method will put the updated timestamp of schema file in the table modified time store map
   */
  private def updateSchemasUpdatedTime(tableUniqueId: String, timeStamp: Long) {
    CarbonFileMetastore.updateTableSchemaModifiedTime(tableUniqueId, timeStamp)
  }

  override def isSchemaRefreshed(absoluteTableIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): Boolean = {
    val localTimeStamp = Option(CarbonFileMetastore.getTableModifiedTime(absoluteTableIdentifier
      .getCarbonTableIdentifier
      .getTableId))
    if (localTimeStamp.isDefined) {
      if (CarbonFileMetastore.checkIfRefreshIsNeeded(absoluteTableIdentifier, localTimeStamp.get)) {
        sparkSession.sessionState
          .catalog.refreshTable(TableIdentifier(absoluteTableIdentifier.getTableName,
            Some(absoluteTableIdentifier.getDatabaseName)))
        true
      } else {
        false
      }
    } else {
      true
    }
  }

  override def isReadFromHiveMetaStore: Boolean = false

  override def listAllTables(sparkSession: SparkSession): Seq[CarbonTable] = {
    CarbonMetadata.getInstance().getAllTables.asScala
  }


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
      case SubqueryAlias(_, c) if SparkSQLUtil.isRelation(c.getClass.getName) =>
        val catalogTable =
          CarbonReflectionUtils.getFieldOfCatalogTable("tableMeta", c).asInstanceOf[CatalogTable]
        if (!CarbonSource.isCarbonDataSource(catalogTable)) {
          throw new NoSuchTableException(tableIdentifier.database.get, tableIdentifier.table)
        }
        val tableLocation = catalogTable.storage.locationUri match {
          case tableLoc@Some(uri) =>
            FileFactory.getUpdatedFilePath(tableLoc.get.toString)
          case None =>
            CarbonEnv.getTablePath(tableIdentifier.database, tableIdentifier.table)(sparkSession)
        }
        new CarbonDatasourceHadoopRelation(sparkSession,
          Array(tableLocation.asInstanceOf[String]),
          catalogTable.storage.properties,
          Option(catalogTable.schema))
      case _ => throw new NoSuchTableException(tableIdentifier.database.get, tableIdentifier.table)
    }
  }

  def removeStaleTimeStampEntries(sparkSession: SparkSession): Unit = {
    val tablesList = sparkSession.sessionState.catalog.listDatabases().flatMap {
      database =>
        sparkSession.sessionState.catalog.listTables(database)
          .map(table => CarbonTable.buildUniqueName(database, table.table))
    }
    val cachedTableList = if (EnvHelper.isLegacy(sparkSession)) {
      // for multi-tenant scenario, it need to check the table unique name
      // ensure the current user own this table
      CarbonMetadata.getInstance().getAllTables.asScala.filter { carbonTable =>
        carbonTable.getTableUniqueName.equals(
          CarbonTable.buildUniqueName(carbonTable.getDatabaseName, carbonTable.getTableName))
      }
    } else {
      CarbonMetadata.getInstance().getAllTables.asScala
    }
    val invalidTableIds = cachedTableList.map {
      case carbonTable if !tablesList.contains(carbonTable.getTableUniqueName) =>
        CarbonMetadata.getInstance().removeTable(carbonTable.getTableUniqueName)
        carbonTable.getTableId
    }
    CarbonFileMetastore.removeStaleEntries(invalidTableIds.toList)
  }

}
