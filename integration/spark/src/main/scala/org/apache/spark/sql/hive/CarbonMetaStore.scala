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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, DataFrame, Dataset, RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.format.SchemaEvolutionEntry

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
   *
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
  def updateTableSchemaForAlter(
      newTableIdentifier: CarbonTableIdentifier,
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
  def updateTableSchema(newTableIdentifier: CarbonTableIdentifier,
      oldTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      carbonStorePath: String)(sparkSession: SparkSession): String

  /**
   * This method will is used to remove the evolution entry in case of failure.
   *
   * @param carbonTableIdentifier
   * @param thriftTableInfo
   * @param absoluteTableIdentifier
   * @param sparkSession
   */
  def revertTableSchemaInAlterFailure(carbonTableIdentifier: CarbonTableIdentifier,
      thriftTableInfo: org.apache.carbondata.format.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier)
    (sparkSession: SparkSession): String


  /**
   * Prepare Thrift Schema from wrapper TableInfo and write to disk
   */
  def saveToDisk(tableInfo: schema.table.TableInfo, tablePath: String)

  /**
   * Generates schema string to save it in hive metastore
   *
   * @param tableInfo
   * @return
   */
  def generateTableSchemaString(tableInfo: schema.table.TableInfo,
      absoluteTableIdentifier: AbsoluteTableIdentifier): String

  /**
   * This method will remove the table meta from catalog metadata array
   *
   * @param dbName
   * @param tableName
   */
  def removeTableFromMetadata(dbName: String, tableName: String): Unit

  def updateMetadataByThriftTable(schemaFilePath: String,
      tableInfo: org.apache.carbondata.format.TableInfo,
      dbName: String, tableName: String, tablePath: String): Unit

  def isTablePathExists(tableIdentifier: TableIdentifier)(sparkSession: SparkSession): Boolean

  def dropTable(tableIdentifier: AbsoluteTableIdentifier)
    (sparkSession: SparkSession)

  def isSchemaRefreshed(absoluteTableIdentifier: AbsoluteTableIdentifier,
      sparkSession: SparkSession): Boolean

  def isReadFromHiveMetaStore: Boolean

  def listAllTables(sparkSession: SparkSession): Seq[CarbonTable]

  def getThriftTableInfo(
      carbonTable: CarbonTable
  ): org.apache.carbondata.format.TableInfo

  /**
   * Method will be used to retrieve or create carbon data source relation
   *
   * @param sparkSession
   * @param tableIdentifier
   * @return
   */
  def createCarbonDataSourceHadoopRelation(
      sparkSession: SparkSession,
      tableIdentifier: TableIdentifier): CarbonDatasourceHadoopRelation

  /**
   * Method will be used retrieve the schema from unresolved relation
   *
   * @param sparkSession
   * @param query
   * @return
   */
  def getSchemaFromUnresolvedRelation(
      sparkSession: SparkSession,
      query: LogicalPlan): StructType = {
    val df: DataFrame = Dataset.ofRows(sparkSession, query)
    df.schema
  }

}
/**
 * Factory for Carbon metastore
 */
object CarbonMetaStoreFactory {

  val LOGGER = LogServiceFactory.getLogService("org.apache.spark.sql.hive.CarbonMetaStoreFactory")

  def createCarbonMetaStore(conf: RuntimeConfig): CarbonMetaStore = {
    val readSchemaFromHiveMetaStore = readSchemaFromHive(conf)
    if (readSchemaFromHiveMetaStore) {
      LOGGER.info("Hive based carbon metastore is enabled")
      new CarbonHiveMetaStore()
    } else {
      LOGGER.info("File based carbon metastore is enabled")
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
