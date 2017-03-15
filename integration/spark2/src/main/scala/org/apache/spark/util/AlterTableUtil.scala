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

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.{CarbonRelation, HiveExternalCatalog}
import org.apache.spark.sql.hive.HiveExternalCatalog._

import org.apache.carbondata.common.logging.LogService
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.format.{SchemaEvolutionEntry, TableInfo}

object AlterTableUtil {
  def validateTableAndAcquireLock(dbName: String,
      tableName: String,
      locksToBeAcquired: List[String],
      LOGGER: LogService)
    (sparkSession: SparkSession): List[ICarbonLock] = {
    val relation =
      CarbonEnv.get.carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      LOGGER.audit(s"Alter table request has failed. " +
                   s"Table $dbName.$tableName does not exist")
      sys.error(s"Table $dbName.$tableName does not exist")
    }
    // acquire the lock first
    val table = relation.tableMeta.carbonTable
    var acquiredLocks = ListBuffer[ICarbonLock]()
    locksToBeAcquired.foreach { lock =>
      acquiredLocks += getLockObject(table, lock, LOGGER)
    }
    acquiredLocks.toList
  }

  /**
   * Given a lock type this method will return a new lock object if not acquired by any other
   * operation
   *
   * @param carbonTable
   * @param lockType
   * @param LOGGER
   * @return
   */
  private def getLockObject(carbonTable: CarbonTable,
      lockType: String,
      LOGGER: LogService): ICarbonLock = {
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
   * @param LOGGER
   */
  def releaseLocks(locks: List[ICarbonLock], LOGGER: LogService): Unit = {
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
   * @param LOGGER
   */
  def releaseLocksManually(locks: List[ICarbonLock],
      locksAcquired: List[String],
      dbName: String,
      tableName: String,
      storeLocation: String,
      LOGGER: LogService): Unit = {
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

  def updateSchemaInfo(carbonTable: CarbonTable,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      thriftTable: TableInfo)(sparkSession: SparkSession, catalog: HiveExternalCatalog): Unit = {
    val dbName = carbonTable.getDatabaseName
    val tableName = carbonTable.getFactTableName
    CarbonEnv.get.carbonMetastore
      .updateTableSchema(carbonTable.getCarbonTableIdentifier,
        thriftTable,
        schemaEvolutionEntry,
        carbonTable.getStorePath)(sparkSession)
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    val schema = CarbonEnv.get.carbonMetastore
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
}
