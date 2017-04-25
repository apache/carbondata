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

package org.apache.spark.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.{CarbonRelation, HiveExternalCatalog}
import org.apache.spark.sql.hive.HiveExternalCatalog._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock}
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}

object AlterTableUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Validates that the table exists and acquires meta lock on it.
   *
   * @param dbName
   * @param tableName
   * @param sparkSession
   * @return
   */
  def validateTableAndAcquireLock(dbName: String,
      tableName: String,
      locksToBeAcquired: List[String])
    (sparkSession: SparkSession): List[ICarbonLock] = {
    val relation =
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Alter table request has failed. " +
                   s"Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    // acquire the lock first
    val table = relation.tableMeta.carbonTable
    val acquiredLocks = ListBuffer[ICarbonLock]()
    try {
      locksToBeAcquired.foreach { lock =>
        acquiredLocks += getLockObject(table, lock)
      }
      acquiredLocks.toList
    } catch {
      case e: Exception =>
        releaseLocks(acquiredLocks.toList)
        throw e
    }
  }

  /**
   * Given a lock type this method will return a new lock object if not acquired by any other
   * operation
   *
   * @param carbonTable
   * @param lockType
   * @return
   */
  private def getLockObject(carbonTable: CarbonTable,
      lockType: String): ICarbonLock = {
    val carbonLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        lockType)
    if (carbonLock.lockWithRetries()) {
      LOGGER.info(s"Successfully acquired the lock $lockType")
    } else {
      sys.error("Table is locked for updation. Please try after some time")
    }
    carbonLock
  }

  /**
   * This method will release the locks acquired for an operation
   *
   * @param locks
   */
  def releaseLocks(locks: List[ICarbonLock]): Unit = {
    locks.foreach { carbonLock =>
      if (carbonLock.unlock()) {
        LOGGER.info("Alter table lock released successfully")
      } else {
        LOGGER.error("Unable to release lock during alter table operation")
      }
    }
  }

  /**
   * This method will release the locks by manually forming a lock path. Specific usage for
   * rename table
   *
   * @param locks
   * @param locksAcquired
   * @param dbName
   * @param tableName
   * @param storeLocation
   */
  def releaseLocksManually(locks: List[ICarbonLock],
      locksAcquired: List[String],
      dbName: String,
      tableName: String,
      storeLocation: String): Unit = {
    val lockLocation = storeLocation + CarbonCommonConstants.FILE_SEPARATOR +
                       dbName + CarbonCommonConstants.FILE_SEPARATOR + tableName
    locks.zip(locksAcquired).foreach { case (carbonLock, lockType) =>
      val lockFilePath = lockLocation + CarbonCommonConstants.FILE_SEPARATOR +
                         lockType
      if (carbonLock.releaseLockManually(lockFilePath)) {
        LOGGER.info(s"Alter table lock released successfully: ${ lockType }")
      } else {
        LOGGER.error("Unable to release lock during alter table operation")
      }
    }
  }

  /**
   * @param carbonTable
   * @param schemaEvolutionEntry
   * @param thriftTable
   * @param sparkSession
   * @param catalog
   */
  def updateSchemaInfo(carbonTable: CarbonTable,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      thriftTable: TableInfo)(sparkSession: SparkSession, catalog: HiveExternalCatalog): Unit = {
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getFactTableName
    CarbonEnv.getInstance(sparkSession).carbonMetastore
      .updateTableSchema(carbonTable.getCarbonTableIdentifier,
        thriftTable,
        schemaEvolutionEntry,
        carbonTable.getStorePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    val schema = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession).schema.json
    val schemaParts = prepareSchemaJsonForAlterTable(sparkSession.sparkContext.getConf, schema)
    catalog.client.runSqlHive(
      s"ALTER TABLE $dbName.$tableName SET TBLPROPERTIES($schemaParts)")
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
  }

  /**
   * This method will split schema string into multiple parts of configured size and
   * registers the parts as keys in tableProperties which will be read by spark to prepare
   * Carbon Table fields
   *
   * @param sparkConf
   * @param schemaJsonString
   * @return
   */
  private def prepareSchemaJsonForAlterTable(sparkConf: SparkConf,
      schemaJsonString: String): String = {
    val threshold = sparkConf
      .getInt(CarbonCommonConstants.SPARK_SCHEMA_STRING_LENGTH_THRESHOLD,
        CarbonCommonConstants.SPARK_SCHEMA_STRING_LENGTH_THRESHOLD_DEFAULT)
    // Split the JSON string.
    val parts = schemaJsonString.grouped(threshold).toSeq
    var schemaParts: Seq[String] = Seq.empty
    schemaParts = schemaParts :+ s"'$DATASOURCE_SCHEMA_NUMPARTS'='${ parts.size }'"
    parts.zipWithIndex.foreach { case (part, index) =>
      schemaParts = schemaParts :+ s"'$DATASOURCE_SCHEMA_PART_PREFIX$index'='$part'"
    }
    schemaParts.mkString(",")
  }

  /**
   * This method reverts the changes to the schema if the rename table command fails.
   *
   * @param oldTableIdentifier
   * @param newTableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertRenameTableChanges(oldTableIdentifier: TableIdentifier,
      newTableName: String,
      storePath: String,
      tableId: String,
      timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    val database = oldTableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val newCarbonTableIdentifier = new CarbonTableIdentifier(database, newTableName, tableId)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(storePath,
      newCarbonTableIdentifier)
    val tableMetadataFile = carbonTablePath.getSchemaFilePath
    val fileType = FileFactory.getFileType(tableMetadataFile)
    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      val tableInfo: org.apache.carbondata.format.TableInfo = CarbonEnv.getInstance(sparkSession)
        .carbonMetastore
        .readSchemaFile(tableMetadataFile)
      val evolutionEntryList = tableInfo.fact_table.schema_evolution.schema_evolution_history
      val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
      if (updatedTime == timeStamp) {
        LOGGER.error(s"Reverting changes for $database.${ oldTableIdentifier.table }")
        FileFactory.getCarbonFile(carbonTablePath.getPath, fileType)
          .renameForce(carbonTablePath.getParent.toString + CarbonCommonConstants.FILE_SEPARATOR +
                       oldTableIdentifier.table)
        val tableIdentifier = new CarbonTableIdentifier(database,
          oldTableIdentifier.table,
          tableId)
        CarbonEnv.getInstance(sparkSession).carbonMetastore.revertTableSchema(tableIdentifier,
          tableInfo,
          storePath)(sparkSession)
        CarbonEnv.getInstance(sparkSession).carbonMetastore
          .removeTableFromMetadata(database, newTableName)
      }
    }
  }

  /**
   * This method reverts the changes to the schema if add column command fails.
   *
   * @param dbName
   * @param tableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertAddColumnChanges(dbName: String, tableName: String, timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable

    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
      carbonTable.getCarbonTableIdentifier)
    val tableMetadataFile = carbonTablePath.getSchemaFilePath
    val thriftTable: TableInfo = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .readSchemaFile(tableMetadataFile)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.info(s"Reverting changes for $dbName.$tableName")
      val addedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).added
      thriftTable.fact_table.table_columns.removeAll(addedSchemas)
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .revertTableSchema(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getStorePath)(sparkSession)
    }
  }

  /**
   * This method reverts the schema changes if drop table command fails.
   *
   * @param dbName
   * @param tableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertDropColumnChanges(dbName: String, tableName: String, timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
      carbonTable.getCarbonTableIdentifier)
    val tableMetadataFile = carbonTablePath.getSchemaFilePath
    val thriftTable: TableInfo = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .readSchemaFile(tableMetadataFile)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.error(s"Reverting changes for $dbName.$tableName")
      val removedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).removed
      thriftTable.fact_table.table_columns.asScala.foreach { columnSchema =>
        removedSchemas.asScala.foreach { removedSchemas =>
          if (columnSchema.invisible && removedSchemas.column_id == columnSchema.column_id) {
            columnSchema.setInvisible(false)
          }
        }
      }
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .revertTableSchema(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getStorePath)(sparkSession)
    }
  }

  /**
   * This method reverts the changes to schema if the data type change command fails.
   *
   * @param dbName
   * @param tableName
   * @param timeStamp
   * @param sparkSession
   */
  def revertDataTypeChanges(dbName: String, tableName: String, timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(Some(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation].tableMeta
      .carbonTable
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getStorePath,
      carbonTable.getCarbonTableIdentifier)
    val tableMetadataFile = carbonTablePath.getSchemaFilePath
    val thriftTable: TableInfo = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .readSchemaFile(tableMetadataFile)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.error(s"Reverting changes for $dbName.$tableName")
      val removedColumns = evolutionEntryList.get(evolutionEntryList.size() - 1).removed
      thriftTable.fact_table.table_columns.asScala.foreach { columnSchema =>
        removedColumns.asScala.foreach { removedColumn =>
          if (columnSchema.column_id.equalsIgnoreCase(removedColumn.column_id) &&
              !columnSchema.isInvisible) {
            columnSchema.setData_type(removedColumn.data_type)
            columnSchema.setPrecision(removedColumn.precision)
            columnSchema.setScale(removedColumn.scale)
          }
        }
      }
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .revertTableSchema(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getStorePath)(sparkSession)
    }
  }

}
