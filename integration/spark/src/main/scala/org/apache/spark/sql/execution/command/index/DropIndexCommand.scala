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

package org.apache.spark.sql.execution.command.index

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.{CarbonHiveIndexMetadataUtil, CarbonRelation}
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore

import org.apache.carbondata.common.exceptions.sql.{MalformedIndexCommandException, NoSuchIndexException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Command to drop index on a table
 */
private[sql] case class DropIndexCommand(
    ifExistsSet: Boolean,
    dbNameOp: Option[String],
    parentTableName: String = null,
    indexName: String)
  extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val parentTable = try {
      CarbonEnv.getCarbonTable(dbNameOp, parentTableName)(sparkSession)
    } catch {
      case e: NoSuchTableException =>
        if (!ifExistsSet) throw e
        else return Seq.empty
    }
    if (!CarbonIndexUtil.getIndexesTables(parentTable).contains(indexName)) {
      if (!ifExistsSet) {
        throw new MalformedIndexCommandException("Index with name " + indexName + " does not exist")
      }
    }
    if (!parentTable.getIndexTableNames(CarbonIndexProvider.SI.getIndexProviderName)
      .contains(indexName)) {
      IndexStoreManager.getInstance().deleteIndex(parentTable, indexName)
      CarbonHiveIndexMetadataUtil.removeIndexInfoFromParentTable(parentTable.getIndexInfo,
        parentTable,
        parentTable.getDatabaseName,
        indexName)(sparkSession)
    } else {
      dropIndexTable(sparkSession)
    }
    Seq.empty
  }

  def dropIndexTable(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    LOGGER.info(s"dropping index table $indexName")
    val dbName = CarbonEnv.getDatabaseName(dbNameOp)(sparkSession)
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
          Some(CarbonEnv.getCarbonTable(Some(dbName), indexName)(sparkSession))
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
              val indexTableList = CarbonIndexUtil.getIndexesTables(parentCarbonTable.get)
              if (!indexTableList.isEmpty) {
                locksToBeAcquired foreach {
                  lock => {
                    carbonLocks += CarbonLockUtil
                      .getLockObject(parentCarbonTable.get.getAbsoluteTableIdentifier, lock)
                  }
                }
                CarbonHiveIndexMetadataUtil.removeIndexInfoFromParentTable(parentCarbonTable
                  .get
                  .getIndexInfo,
                  parentCarbonTable.get,
                  dbName,
                  indexName)(sparkSession)
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
              throw new NoSuchIndexException(indexName)
            }
            None
        }

      if (carbonTable.isDefined) {
        CarbonInternalMetastore.refreshIndexInfo(dbName, indexName, carbonTable.get)(sparkSession)
        val isIndexTableBool = carbonTable.get.isIndexTable
        val parentTableName = CarbonIndexUtil.getParentTableName(carbonTable.get)
        var parentTable = CarbonEnv.getCarbonTable(Some(dbName), parentTableName)(sparkSession)
        if (!isIndexTableBool) {
          sys.error(s"Drop Index command is not permitted on table [$dbName.$indexName]")
        } else if (isIndexTableBool &&
                   !parentTableName.equalsIgnoreCase(parentTableName)) {
          throw new NoSuchIndexException(indexName)
        } else {
          if (isIndexTableBool) {
            tableIdentifierForAcquiringLock = parentTable.getAbsoluteTableIdentifier
          } else {
            tableIdentifierForAcquiringLock = AbsoluteTableIdentifier
              .from(carbonTable.get.getTablePath, dbName.toLowerCase, indexName.toLowerCase)
          }
          locksToBeAcquired foreach {
            lock => {
              carbonLocks += CarbonLockUtil.getLockObject(tableIdentifierForAcquiringLock, lock)
            }
          }
          isValidDeletion = true
        }

        val tableIdentifier = TableIdentifier(indexName, Some(dbName))
        // drop carbon table
        val tablePath = carbonTable.get.getTablePath

        CarbonInternalMetastore.dropIndexTable(tableIdentifier, carbonTable.get,
          tablePath,
          parentTable,
          removeEntryFromParentTable = true)(sparkSession)

        // take the refreshed table object after dropping and updating the index table
        parentTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(Some(dbName), parentTableName)(sparkSession).asInstanceOf[CarbonRelation]
          .carbonTable

        val indexTables = CarbonIndexUtil.getIndexesTables(parentTable)
        // if all the indexes are dropped then the main table holds no index tables,
        // so change the "indexTableExists" property to false, iff all the indexes are deleted
        if (null == indexTables || indexTables.isEmpty) {
          val tableIdentifier = TableIdentifier(parentTable.getTableName,
            Some(parentTable.getDatabaseName))
          // modify the tableProperties of mainTable by adding "indexTableExists" property
          CarbonIndexUtil
            .addOrModifyTableProperty(parentTable,
              Map("indexTableExists" -> "false"), needLock = false)(sparkSession)

          CarbonHiveIndexMetadataUtil.refreshTable(dbName, parentTableName, sparkSession)
        }
      }
    } finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          logInfo("Table MetaData Unlocked Successfully")
          if (isValidDeletion) {
            if (carbonTable != null && carbonTable.isDefined) {
              CarbonInternalMetastore.deleteTableDirectory(dbName, indexName, sparkSession)
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
        CarbonInternalMetastore.deleteTableDirectory(dbName, indexName, sparkSession)
      }
    }
    Seq.empty
  }

}
