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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.format
import org.apache.carbondata.format.SchemaEvolutionEntry
import org.apache.carbondata.spark.util.CarbonSparkUtil

/**
 * Metastore to store carbonschema in hive
 */
class CarbonHiveMetaStore extends CarbonFileMetastore {

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
    val carbonRelation = if (info != null) {
      val table = CarbonTable.buildFromTableInfo(info)
      CarbonRelation(info.getDatabaseName, info.getFactTable.getTableName, table)
    } else {
      super.createCarbonRelation(parameters, absIdentifier, sparkSession)
    }
    carbonRelation.refresh()
    carbonRelation
  }

  override def isSchemaRefreshed(absoluteTableIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): Boolean = true

  override def isTablePathExists(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): Boolean = {
    tableExists(tableIdentifier)(sparkSession)
  }

  override def dropTable(absoluteTableIdentifier: AbsoluteTableIdentifier)
    (sparkSession: SparkSession): Unit = {
    val dbName = absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName
    val tableName = absoluteTableIdentifier.getCarbonTableIdentifier.getTableName
    CarbonHiveMetadataUtil.invalidateAndDropTable(dbName, tableName, sparkSession)
    // discard cached table info in cachedDataSourceTables
    val tableIdentifier = TableIdentifier(tableName, Option(dbName))
    sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
    DataMapStoreManager.getInstance().clearDataMaps(absoluteTableIdentifier)
    SegmentPropertiesAndSchemaHolder.getInstance().invalidate(absoluteTableIdentifier)
    removeTableFromMetadata(dbName, tableName)
  }

  override def listAllTables(sparkSession: SparkSession): Seq[CarbonTable] = {
    // Todo
    Seq()
  }

  override def getThriftTableInfo(carbonTable: CarbonTable): format.TableInfo = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    schemaConverter.fromWrapperToExternalTableInfo(carbonTable.getTableInfo,
      carbonTable.getDatabaseName,
      carbonTable.getTableName)
  }


  /**
   * This method will overwrite the existing schema and update it with the given details
   *
   * @param newTableIdentifier
   * @param thriftTableInfo
   * @param schemaEvolutionEntry
   * @param sparkSession
   */
  override def updateTableSchemaForAlter(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      tablePath: String)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    if (schemaEvolutionEntry != null) {
      thriftTableInfo.fact_table.schema_evolution.schema_evolution_history.add(schemaEvolutionEntry)
    }
    updateHiveMetaStoreForAlter(newTableIdentifier,
      oldTableIdentifier,
      thriftTableInfo,
      tablePath,
      sparkSession,
      schemaConverter)
  }

  /**
   * This method will overwrite the existing schema and update it with the given details
   *
   * @param newTableIdentifier
   * @param thriftTableInfo
   * @param carbonTablePath
   * @param sparkSession
   */
  override def updateTableSchema(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      carbonTablePath: String)(sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    updateHiveMetaStoreForAlter(
      newTableIdentifier,
      oldTableIdentifier,
      thriftTableInfo,
      carbonTablePath,
      sparkSession,
      schemaConverter)
  }

  private def updateHiveMetaStoreForAlter(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: format.TableInfo,
      tablePath: String,
      sparkSession: SparkSession,
      schemaConverter: ThriftWrapperSchemaConverterImpl) = {
    val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
      thriftTableInfo,
      newTableIdentifier.getDatabaseName,
      newTableIdentifier.getTableName,
      tablePath)
    val dbName = newTableIdentifier.getDatabaseName
    val tableName = newTableIdentifier.getTableName
    val schemaParts = CarbonUtil.convertToMultiGsonStrings(wrapperTableInfo, "=", "'", "")
    val hiveClient = sparkSession
      .sessionState
      .catalog
      .externalCatalog.asInstanceOf[HiveExternalCatalog]
      .client
    hiveClient.runSqlHive(s"ALTER TABLE $dbName.$tableName SET SERDEPROPERTIES($schemaParts)")

    sparkSession.catalog.refreshTable(TableIdentifier(tableName, Some(dbName)).quotedString)
    removeTableFromMetadata(dbName, tableName)
    CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo)
    tablePath
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
    CarbonUtil.convertToMultiGsonStrings(tableInfo, " ", "", ",")
  }

  /**
   * This method will is used to remove the evolution entry in case of failure.
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param sparkSession
   */
  override def revertTableSchemaInAlterFailure(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: format.TableInfo,
      identifier: AbsoluteTableIdentifier)
    (sparkSession: SparkSession): String = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val evolutionEntries = thriftTableInfo.fact_table.schema_evolution.schema_evolution_history
    evolutionEntries.remove(evolutionEntries.size() - 1)
    updateHiveMetaStoreForAlter(carbonTableIdentifier,
      carbonTableIdentifier,
      thriftTableInfo,
      identifier.getTablePath,
      sparkSession,
      schemaConverter)
  }

}
