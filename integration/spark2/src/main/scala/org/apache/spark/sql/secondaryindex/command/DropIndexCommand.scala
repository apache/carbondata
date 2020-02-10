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

package org.apache.spark.sql.secondaryindex.command

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, CarbonRelation}
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Command to drop secondary index on a table
 */
private[sql] case class DropIndexCommand(ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    parentTableName: String = null)
  extends RunnableCommand {


  def run(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    var tableIdentifierForAcquiringLock: AbsoluteTableIdentifier = null
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    // flag to check if folders and files can be successfully deleted
    var isValidDeletion = false
    val carbonLocks: scala.collection.mutable.ArrayBuffer[ICarbonLock] = ArrayBuffer[ICarbonLock]()
    var carbonTable: Option[CarbonTable] = None
    try {
      carbonTable =
        try {
          Some(CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession))
        } catch {
          case ex: NoSuchTableException =>
            var isIndexTableExists = false
            // even if the index table does not exists
            // check if the parent table exists and remove the index table reference
            // in case if the parent table hold the deleted index table reference
            try {
              val parentCarbonTable = Some(catalog
                .lookupRelation(Some(dbName), parentTableName)(sparkSession)
                .asInstanceOf[CarbonRelation].carbonTable)
              val indexTableList = CarbonInternalScalaUtil.getIndexesTables(parentCarbonTable.get)
              if (!indexTableList.isEmpty) {
                locksToBeAcquired foreach {
                  lock => {
                    carbonLocks += CarbonLockUtil
                      .getLockObject(parentCarbonTable.get.getAbsoluteTableIdentifier, lock)
                  }
                }
                CarbonHiveMetadataUtil.removeIndexInfoFromParentTable(parentCarbonTable
                  .get
                  .getIndexInfo,
                  parentCarbonTable.get,
                  dbName,
                  tableName)(sparkSession)
                // clear parent table from meta store cache as it is also required to be
                // refreshed when SI table is dropped
                CarbonInternalMetastore
                  .removeTableFromMetadataCache(dbName, parentTableName)(sparkSession)
                isIndexTableExists = true
              }
            } catch {
              case ex: NoSuchTableException =>
                if (!ifExistsSet) {
                  throw ex
                }
              case e: Exception =>
                throw e
            }
            if (!ifExistsSet && !isIndexTableExists) {
              throw ex
            }
            None
        }

      if (carbonTable.isDefined) {
        CarbonInternalMetastore.refreshIndexInfo(dbName, tableName, carbonTable.get)(sparkSession)
        val isIndexTableBool = carbonTable.get.isIndexTable
        val parentTableName = CarbonInternalScalaUtil.getParentTableName(carbonTable.get)
        var parentCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(Some(dbName), parentTableName)(sparkSession).asInstanceOf[CarbonRelation]
          .carbonTable
        if (!isIndexTableBool) {
          sys.error(s"Drop Index command is not permitted on carbon table [$dbName.$tableName]")
        } else if (isIndexTableBool &&
                   !parentTableName.equalsIgnoreCase(parentTableName)) {
          sys.error(s"Index Table [$dbName.$tableName] does not exist on " +
                    s"parent table [$dbName.$parentTableName]")
        } else {
          if (isIndexTableBool) {
            tableIdentifierForAcquiringLock = parentCarbonTable.getAbsoluteTableIdentifier
          } else {
            tableIdentifierForAcquiringLock = AbsoluteTableIdentifier
              .from(carbonTable.get.getTablePath, dbName.toLowerCase, tableName.toLowerCase)
          }
          locksToBeAcquired foreach {
            lock => {
              carbonLocks += CarbonLockUtil.getLockObject(tableIdentifierForAcquiringLock, lock)
            }
          }
          isValidDeletion = true
        }

        val tableIdentifier = TableIdentifier(tableName, Some(dbName))
        // drop carbon table
        val tablePath = carbonTable.get.getTablePath

        CarbonInternalMetastore.dropIndexTable(tableIdentifier, carbonTable.get,
          tablePath,
          parentCarbonTable,
          removeEntryFromParentTable = true)(sparkSession)

        // take the refreshed table object after dropping and updating the index table
        parentCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(Some(dbName), parentTableName)(sparkSession).asInstanceOf[CarbonRelation]
          .carbonTable

        val indexTables = CarbonInternalScalaUtil.getIndexesTables(parentCarbonTable)
        // if all the indexes are dropped then the main table holds no index tables,
        // so change the "indexTableExists" property to false, iff all the indexes are deleted
        if (null == indexTables || indexTables.isEmpty) {
          val tableIdentifier = TableIdentifier(parentCarbonTable.getTableName,
            Some(parentCarbonTable.getDatabaseName))
          // modify the tableProperties of mainTable by adding "indexTableExists" property
          CarbonInternalScalaUtil
            .addOrModifyTableProperty(parentCarbonTable,
              Map("indexTableExists" -> "false"), needLock = false)(sparkSession)

          CarbonHiveMetadataUtil.refreshTable(dbName, parentTableName, sparkSession)
        }
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Dropping table $dbName.$tableName failed", ex)
        if (!ifExistsSet) {
          sys.error(s"Dropping table $dbName.$tableName failed: ${ ex.getMessage }")
        }
    } finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          logInfo("Table MetaData Unlocked Successfully")
          if (isValidDeletion) {
            if (carbonTable != null && carbonTable.isDefined) {
              CarbonInternalMetastore.deleteTableDirectory(dbName, tableName, sparkSession)
            }
          }
        } else {
          logError("Table metadata unlocking is unsuccessful, index table may be in stale state")
        }
      }
      // in case if the the physical folders still exists for the index table
      // but the carbon and hive info for the index table is removed,
      // DROP INDEX IF EXISTS should clean up those physical directories
      if (ifExistsSet && carbonTable.isEmpty) {
        CarbonInternalMetastore.deleteTableDirectory(dbName, tableName, sparkSession)
      }
    }
    Seq.empty
  }
}
