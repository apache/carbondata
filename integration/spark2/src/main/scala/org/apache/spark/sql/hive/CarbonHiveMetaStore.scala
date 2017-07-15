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

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.format
import org.apache.carbondata.format.SchemaEvolutionEntry
import org.apache.carbondata.processing.merger.TableMeta
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * Metastore to store carbonschema in hive
 */
class CarbonHiveMetaStore(conf: RuntimeConfig, storePath: String)
  extends CarbonFileMetastore(conf, storePath) {

  override def isReadFromHiveMetaStore: Boolean = true


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
    val info = CarbonUtil.convertGsonToTableInfo(parameters.asJava)
    if (info != null) {
      val table = CarbonTable.buildFromTableInfo(info)
      val meta = new TableMeta(table.getCarbonTableIdentifier,
        table.getStorePath, table)
      CarbonRelation(info.getDatabaseName, info.getFactTable.getTableName,
        CarbonSparkUtil.createSparkMeta(table), meta)
    } else {
      super.createCarbonRelation(parameters, absIdentifier, sparkSession)
    }
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
    relation
  }

  /**
   * This method will search for a table in the catalog metadata
   *
   * @param database
   * @param tableName
   * @return
   */
  override def getTableFromMetadata(database: String,
      tableName: String,
      readStore: Boolean): Option[TableMeta] = {
    if (!readStore) {
      None
    } else {
      super.getTableFromMetadata(database, tableName, readStore)
    }
  }


  override def isTablePathExists(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): Boolean = {
    tableExists(tableIdentifier)(sparkSession)
  }

  override def dropTable(tableStorePath: String, tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): Unit = {
    val dbName = tableIdentifier.database.get
    val tableName = tableIdentifier.table
    val carbonTable = CarbonMetadata.getInstance.getCarbonTable(dbName + "_" + tableName)
    if (null != carbonTable) {
      // clear driver B-tree and dictionary cache
      ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(carbonTable)
    }
    checkSchemasModifiedTimeAndReloadTables
    removeTableFromMetadata(dbName, tableName)
    CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
    // discard cached table info in cachedDataSourceTables
    sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
  }

  override def checkSchemasModifiedTimeAndReloadTables(): Unit = {
    // do nothing now
  }

  override def listAllTables(sparkSession: SparkSession): Seq[CarbonTable] = {
    // Todo
    Seq()
  }

  override def getThriftTableInfo(tablePath: CarbonTablePath)
    (sparkSession: SparkSession): format.TableInfo = {
    val identifier = tablePath.getCarbonTableIdentifier
    val relation = lookupRelation(TableIdentifier(identifier.getTableName,
      Some(identifier.getDatabaseName)))(sparkSession).asInstanceOf[CarbonRelation]
    val carbonTable = relation.metaData.carbonTable
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    schemaConverter.fromWrapperToExternalTableInfo(carbonTable.getTableInfo,
      carbonTable.getDatabaseName,
      carbonTable.getFactTableName)
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
  override def updateTableSchema(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      carbonStorePath: String)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    if (schemaEvolutionEntry != null) {
      thriftTableInfo.fact_table.schema_evolution.schema_evolution_history.add(schemaEvolutionEntry)
    }
    updateHiveMetaStore(newTableIdentifier,
      oldTableIdentifier,
      thriftTableInfo,
      carbonStorePath,
      sparkSession,
      schemaConverter)
  }

  private def updateHiveMetaStore(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: format.TableInfo,
      carbonStorePath: String,
      sparkSession: SparkSession,
      schemaConverter: ThriftWrapperSchemaConverterImpl) = {
    val wrapperTableInfo = schemaConverter
      .fromExternalToWrapperTableInfo(thriftTableInfo,
        newTableIdentifier.getDatabaseName,
        newTableIdentifier.getTableName,
        carbonStorePath)
    wrapperTableInfo.setStorePath(storePath)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath, newTableIdentifier)
    val schemaMetadataPath =
      CarbonTablePath.getFolderContainingFile(carbonTablePath.getSchemaFilePath)
    wrapperTableInfo.setMetaDataFilepath(schemaMetadataPath)
    val dbName = oldTableIdentifier.getDatabaseName
    val tableName = oldTableIdentifier.getTableName
    val carbonUpdatedIdentifier = new CarbonTableIdentifier(dbName, tableName,
      wrapperTableInfo.getFactTable.getTableId)
    val schemaParts = CarbonUtil.convertToMultiGsonStrings(wrapperTableInfo, "=", "'", "")
    sparkSession.sessionState.asInstanceOf[CarbonSessionState].metadataHive.runSqlHive(
      s"ALTER TABLE $dbName.$tableName SET SERDEPROPERTIES($schemaParts)")
    sparkSession.catalog.refreshTable(TableIdentifier(tableName, Some(dbName)).quotedString)
    removeTableFromMetadata(wrapperTableInfo.getDatabaseName,
      wrapperTableInfo.getFactTable.getTableName)
    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    CarbonStorePath.getCarbonTablePath(storePath, carbonUpdatedIdentifier).getPath
  }

  /**
   * This method will is used to remove the evolution entry in case of failure.
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param carbonStorePath
   * @param sparkSession
   */
  override def revertTableSchema(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: format.TableInfo,
      carbonStorePath: String)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val evolutionEntries = thriftTableInfo.fact_table.schema_evolution.schema_evolution_history
    evolutionEntries.remove(evolutionEntries.size() - 1)
    updateHiveMetaStore(carbonTableIdentifier,
      carbonTableIdentifier,
      thriftTableInfo,
      carbonStorePath,
      sparkSession,
      schemaConverter)
  }


}
