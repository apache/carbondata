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

import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.processing.merger.TableMeta

/**
 * Interface for Carbonmetastore
 */
trait CarbonMetaStore {

  def lookupRelation(dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): LogicalPlan

  def lookupRelation(tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession): LogicalPlan

  /**
   * Create spark session from paramters.
   * @param parameters
   * @param absIdentifier
   * @param sparkSession
   */
  def createCarbonRelation(parameters: Map[String, String],
      absIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): CarbonRelation


  def tableExists(
    table: String,
    databaseOp: Option[String] = None)(sparkSession: SparkSession): Boolean

  def tableExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean

  /**
   * This method will overwrite the existing schema and update it with the given details
   *
   * @param newTableIdentifier
   * @param thriftTableInfo
   * @param schemaEvolutionEntry
   * @param carbonStorePath
   * @param sparkSession
   */
  def updateTableSchemaForAlter(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      carbonStorePath: String)(sparkSession: SparkSession): String

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
      carbonStorePath: String)(sparkSession: SparkSession): String

  /**
   * This method will is used to remove the evolution entry in case of failure.
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param tablePath
   * @param sparkSession
   */
  def revertTableSchemaInAlterFailure(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      tablePath: String)
    (sparkSession: SparkSession): String


  def revertTableSchemaForPreAggCreationFailure(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      tablePath: String)(sparkSession: SparkSession): String
  /**
   * Prepare Thrift Schema from wrapper TableInfo and write to disk
   */
  def saveToDisk(tableInfo: schema.table.TableInfo, tablePath: String)

  /**
   * Generates schema string to save it in hive metastore
   * @param tableInfo
   * @return
   */
  def generateTableSchemaString(tableInfo: schema.table.TableInfo,
      tablePath: String): String

  /**
   * This method will remove the table meta from catalog metadata array
   *
   * @param dbName
   * @param tableName
   */
  def removeTableFromMetadata(dbName: String, tableName: String): Unit

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, tablePath: String): Unit

  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean

  def dropTable(tablePath: String, tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession)

  def updateAndTouchSchemasUpdatedTime(basePath: String)

  def checkSchemasModifiedTimeAndReloadTables(storePath: String)

  def isReadFromHiveMetaStore : Boolean

  def listAllTables(sparkSession: SparkSession): Seq[CarbonTable]

  def getThriftTableInfo(tablePath: CarbonTablePath)(sparkSession: SparkSession): TableInfo

  def getTableFromMetadataCache(database: String, tableName: String): Option[TableMeta]

}

/**
 * Factory for Carbon metastore
 */
object CarbonMetaStoreFactory {

  def createCarbonMetaStore(conf: RuntimeConfig): CarbonMetaStore = {
    val readSchemaFromHiveMetaStore = readSchemaFromHive(conf)
    if (readSchemaFromHiveMetaStore) {
      new CarbonHiveMetaStore()
    } else {
      new CarbonFileMetastore()
    }
  }

  def readSchemaFromHive(conf: RuntimeConfig): Boolean = {
    val readSchemaFromHive = {
      if (conf.contains(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE)) {
        conf.get(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE)
      } else if (System.getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE) != null) {
        System.getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE)
      } else {
        CarbonProperties.getInstance().
          getProperty(CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE,
          CarbonCommonConstants.ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT)
      }
    }
    readSchemaFromHive.toBoolean
  }

}
