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
import org.apache.carbondata.core.metadata.schema.table
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.processing.merger.TableMeta

/**
 * Interface for Carbonmetastore
 */
trait CarbonMetaStore {

  def lookupRelation(dbName: Option[String], tableName: String)
    (sparkSession: SparkSession): LogicalPlan

  def lookupRelation(tableIdentifier: TableIdentifier, readFromStore: Boolean = false)
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

  /**
   * Get table meta
   * TODO remove it if possible
   * @param database
   * @param tableName
   * @param readStore
   * @return
   */
  def getTableFromMetadata(database: String,
      tableName: String,
      readStore: Boolean = false): Option[TableMeta]

  def tableExists(
    table: String,
    databaseOp: Option[String] = None)(sparkSession: SparkSession): Boolean

  def tableExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean

  def loadMetadata(metadataPath: String, queryId: String): MetaData

  /**
   * This method will overwrite the existing schema and update it with the given details
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param schemaEvolutionEntry
   * @param carbonStorePath
   * @param sparkSession
   */
  def updateTableSchema(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      carbonStorePath: String)
    (sparkSession: SparkSession): String

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
    (sparkSession: SparkSession): String

  /**
   *
   * Prepare Thrift Schema from wrapper TableInfo and write to Schema file.
   * Load CarbonTable from wrapper tableInfo
   *
   */
  def createTableFromThrift(tableInfo: table.TableInfo,
      dbName: String,
      tableName: String)(sparkSession: SparkSession): (String, String)

  /**
   * This method will remove the table meta from catalog metadata array
   *
   * @param dbName
   * @param tableName
   */
  def removeTableFromMetadata(dbName: String, tableName: String): Unit

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: TableInfo, dbName: String, tableName: String, storePath: String): Unit

  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean

  def dropTable(tableStorePath: String, tableIdentifier: TableIdentifier)
    (sparkSession: SparkSession)

  def updateAndTouchSchemasUpdatedTime(databaseName: String, tableName: String)

  def checkSchemasModifiedTimeAndReloadTables()

  def isReadFromHiveMetaStore : Boolean

  def listAllTables(sparkSession: SparkSession): Seq[CarbonTable]

  def storePath: String

}

/**
 * Factory for Carbon metastore
 */
object CarbonMetaStoreFactory {

  def createCarbonMetaStore(conf: RuntimeConfig, storePath: String): CarbonMetaStore = {
    val readSchemaFromHiveMetaStore = readSchemaFromHive(conf)
    if (readSchemaFromHiveMetaStore) {
      new CarbonHiveMetaStore(conf, storePath)
    } else {
      new CarbonFileMetastore(conf, storePath)
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
