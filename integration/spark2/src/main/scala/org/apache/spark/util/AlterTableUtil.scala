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
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionState}
import org.apache.spark.sql.hive.HiveExternalCatalog._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
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
    val table = relation.carbonTable
    val acquiredLocks = ListBuffer[ICarbonLock]()
    try {
      locksToBeAcquired.foreach { lock =>
        acquiredLocks += CarbonLockUtil.getLockObject(table.getAbsoluteTableIdentifier, lock)
      }
      acquiredLocks.toList
    } catch {
      case e: Exception =>
        releaseLocks(acquiredLocks.toList)
        throw e
    }
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
   * @param tablePath
   */
  def releaseLocksManually(locks: List[ICarbonLock],
      locksAcquired: List[String],
      dbName: String,
      tableName: String,
      tablePath: String): Unit = {
    val lockLocation = tablePath
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
   * @param sessionState
   */
  def updateSchemaInfo(carbonTable: CarbonTable,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      thriftTable: TableInfo)(sparkSession: SparkSession,
      sessionState: CarbonSessionState): Unit = {
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getTableName
    CarbonEnv.getInstance(sparkSession).carbonMetastore
      .updateTableSchemaForAlter(carbonTable.getCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        thriftTable,
        schemaEvolutionEntry,
        carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
    val schema = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession).schema.json
    val schemaParts = prepareSchemaJsonForAlterTable(sparkSession.sparkContext.getConf, schema)
    sessionState.metadataHive.runSqlHive(
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
      tablePath: String,
      tableId: String,
      timeStamp: Long)
    (sparkSession: SparkSession): Unit = {
    val database = oldTableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    val oldCarbonTableIdentifier = new CarbonTableIdentifier(database,
      oldTableIdentifier.table, tableId)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(tablePath, oldCarbonTableIdentifier)
    val newCarbonTableIdentifier = new CarbonTableIdentifier(database, newTableName, tableId)
    val newTablePath = CarbonUtil.getNewTablePath(new Path(tablePath), newCarbonTableIdentifier)
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val tableMetadataFile = carbonTablePath.getPath
    val fileType = FileFactory.getFileType(tableMetadataFile)
    if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
      val tableInfo = if (metastore.isReadFromHiveMetaStore) {
        // In case of hive metastore we first update the carbonschema inside old table only.
        metastore.getThriftTableInfo(CarbonStorePath.getCarbonTablePath(tablePath,
          new CarbonTableIdentifier(database, oldTableIdentifier.table, tableId)))(sparkSession)
      } else {
        metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
      }
      val evolutionEntryList = tableInfo.fact_table.schema_evolution.schema_evolution_history
      val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
      if (updatedTime == timeStamp) {
        LOGGER.error(s"Reverting changes for $database.${ oldTableIdentifier.table }")
        FileFactory.getCarbonFile(carbonTablePath.getPath, fileType)
          .renameForce(carbonTablePath.getParent.toString + CarbonCommonConstants.FILE_SEPARATOR +
                       oldTableIdentifier.table)
        val absoluteTableIdentifier = new AbsoluteTableIdentifier(newTablePath,
          newCarbonTableIdentifier)
        metastore.revertTableSchemaInAlterFailure(oldCarbonTableIdentifier,
          tableInfo, absoluteTableIdentifier)(sparkSession)
        metastore.removeTableFromMetadata(database, newTableName)
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
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getTablePath,
      carbonTable.getCarbonTableIdentifier)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
    val evolutionEntryList = thriftTable.fact_table.schema_evolution.schema_evolution_history
    val updatedTime = evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp
    if (updatedTime == timeStamp) {
      LOGGER.info(s"Reverting changes for $dbName.$tableName")
      val addedSchemas = evolutionEntryList.get(evolutionEntryList.size() - 1).added
      thriftTable.fact_table.table_columns.removeAll(addedSchemas)
      metastore
        .revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
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
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getTablePath,
      carbonTable.getCarbonTableIdentifier)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
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
      metastore
        .revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
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
    val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getTablePath,
      carbonTable.getCarbonTableIdentifier)
    val thriftTable: TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
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
      metastore
        .revertTableSchemaInAlterFailure(carbonTable.getCarbonTableIdentifier,
          thriftTable, carbonTable.getAbsoluteTableIdentifier)(sparkSession)
    }
  }

  /**
   * This method add/modify the table comments.
   *
   * @param tableIdentifier
   * @param properties
   * @param propKeys
   * @param set
   * @param sparkSession
   * @param sessionState
   */
  def modifyTableComment(tableIdentifier: TableIdentifier, properties: Map[String, String],
                         propKeys: Seq[String], set: Boolean)
                        (sparkSession: SparkSession, sessionState: CarbonSessionState): Unit = {
    val tableName = tableIdentifier.table
    val dbName = tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table comment request has been received for $dbName.$tableName")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    var timeStamp = 0L
    var carbonTable: CarbonTable = null
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      // get the latest carbon table
      // read the latest schema file
      val carbonTablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getTablePath,
        carbonTable.getCarbonTableIdentifier)
      val thriftTableInfo: TableInfo = metastore.getThriftTableInfo(carbonTablePath)(sparkSession)
      val schemaConverter = new ThriftWrapperSchemaConverterImpl()
      val wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(
        thriftTableInfo,
        dbName,
        tableName,
        carbonTable.getTablePath)
      val schemaEvolutionEntry = new org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
      schemaEvolutionEntry.setTimeStamp(timeStamp)
      val thriftTable = schemaConverter.fromWrapperToExternalTableInfo(
        wrapperTableInfo, dbName, tableName)
      val tblPropertiesMap: mutable.Map[String, String] =
        thriftTable.fact_table.getTableProperties.asScala
      if (set) {
        //       This overrides old properties and update the comment parameter of thriftTable
        //       with the newly added/modified comment since thriftTable also holds comment as its
        //       direct property.

        properties.foreach { x =>
          if (x._1.equalsIgnoreCase(CarbonCommonConstants.TABLE_COMMENT)) {
            tblPropertiesMap.put(x._1, x._2)
          }
        }
      } else {
        // This removes the comment parameter from thriftTable
        // since thriftTable also holds comment as its property.
        propKeys.foreach { x =>
          if (x.equalsIgnoreCase(CarbonCommonConstants.TABLE_COMMENT)) {
            tblPropertiesMap.remove(x)
          }
        }
      }
      updateSchemaInfo(carbonTable,
        schemaConverter.fromWrapperToExternalSchemaEvolutionEntry(schemaEvolutionEntry),
        thriftTable)(sparkSession, sessionState)
      LOGGER.info(s"Alter table comment is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table comment is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        LOGGER.error(e, "Alter table comment failed")
        sys.error(s"Alter table comment operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
  }
}
