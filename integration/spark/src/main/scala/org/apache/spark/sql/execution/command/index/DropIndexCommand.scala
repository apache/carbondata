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
import org.apache.spark.sql.hive.{CarbonHiveIndexMetadataUtil, CarbonMetaStore, CarbonRelation}
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore

import org.apache.carbondata.common.exceptions.sql.{MalformedIndexCommandException, NoSuchIndexException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Command to drop index on a table
 */
private[sql] case class DropIndexCommand(
    ifExistsSet: Boolean,
    dbNameOp: Option[String],
    parentTableName: String = null,
    indexName: String,
    needLock: Boolean = true)
  extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    var isSecondaryIndex = false
    val parentTable = try {
      CarbonEnv.getCarbonTable(dbNameOp, parentTableName)(sparkSession)
    } catch {
      case e: NoSuchTableException =>
        if (!ifExistsSet) throw e
        else return Seq.empty
    }
    if (!parentTable.getIndexTableNames().contains(indexName)) {
      if (!ifExistsSet) {
        throw new MalformedIndexCommandException("Index with name " + indexName + " does not exist")
      } else {
      return Seq.empty
      }
    }
    if (parentTable.getIndexTableNames(IndexType.SI.getIndexProviderName)
      .contains(indexName)) {
      isSecondaryIndex = true
    } else {
      // Clear the index of a table from memory and disk
      IndexStoreManager.getInstance().deleteIndex(parentTable, indexName)
    }
    dropIndex(isSecondaryIndex, sparkSession)
    Seq.empty
  }

  def dropIndex(isSecondaryIndex: Boolean, sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val dbName = CarbonEnv.getDatabaseName(dbNameOp)(sparkSession)
    var tableIdentifierForAcquiringLock: AbsoluteTableIdentifier = null
    val locksToBeAcquired = if (needLock) {
      List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
    } else {
      List.empty
    }
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    // flag to check if folders and files can be successfully deleted
    var isValidDeletion = false
    val carbonLocks: scala.collection.mutable.ArrayBuffer[ICarbonLock] = ArrayBuffer[ICarbonLock]()
    var carbonTable: Option[CarbonTable] = None
    try {
      if (isSecondaryIndex) {
        LOGGER.info(s"dropping index table $indexName")
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
                val parentCarbonTable = getParentTableFromCatalog(sparkSession, dbName, catalog)
                val indexTableList = CarbonIndexUtil.getSecondaryIndexes(parentCarbonTable.get)
                if (!indexTableList.isEmpty) {
                  removeIndexInfoFromParentTable(sparkSession,
                    dbName,
                    locksToBeAcquired,
                    carbonLocks,
                    parentCarbonTable.get)
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
          parentTable = getRefreshedParentTable(sparkSession, dbName)

          val indexTables = CarbonIndexUtil.getSecondaryIndexes(parentTable)
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
      } else {
        // remove cg or fg index
        var parentCarbonTable = getParentTableFromCatalog(sparkSession, dbName, catalog).get
        removeIndexInfoFromParentTable(sparkSession,
          dbName,
          locksToBeAcquired,
          carbonLocks,
          parentCarbonTable)
        parentCarbonTable = getRefreshedParentTable(sparkSession, dbName)
        val indexMetadata = parentCarbonTable.getIndexMetadata
        var hasCgFgIndexes = false
        if (null != indexMetadata && null != indexMetadata.getIndexesMap) {
          // check if any CG or FG index exists. If not exists,
          // then set indexExists as false to return empty index list for next query.
          hasCgFgIndexes =
            indexMetadata.getIndexesMap.containsKey(IndexType.BLOOMFILTER.getIndexProviderName) ||
              indexMetadata.getIndexesMap.containsKey(IndexType.LUCENE.getIndexProviderName)
        }
        if (!hasCgFgIndexes) {
          CarbonIndexUtil
            .addOrModifyTableProperty(parentCarbonTable,
              Map("indexExists" -> "false"), needLock = false)(sparkSession)

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
              CarbonInternalMetastore.deleteTableDirectory(carbonTable.get)
            }
          }
        } else {
          logError("Table metadata unlocking is unsuccessful, index table may be in stale state")
        }
      }
    }
    Seq.empty
  }

  /**
   * get the refreshed table object after dropping the index
   */
  private def getRefreshedParentTable(sparkSession: SparkSession,
      dbName: String) = {
    CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .lookupRelation(Some(dbName), parentTableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable
  }

  private def getParentTableFromCatalog(sparkSession: SparkSession,
      dbName: String,
      catalog: CarbonMetaStore): Option[CarbonTable] = {
    Some(catalog.lookupRelation(Some(dbName), parentTableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable)
  }

  /**
   * AcquireLock and remove indexInfo from parent table
   */
  private def removeIndexInfoFromParentTable(sparkSession: SparkSession,
      dbName: String,
      locksToBeAcquired: List[String],
      carbonLocks: ArrayBuffer[ICarbonLock],
      parentCarbonTable: CarbonTable): Unit = {
    locksToBeAcquired foreach {
      lock => {
        carbonLocks += CarbonLockUtil
          .getLockObject(parentCarbonTable.getAbsoluteTableIdentifier, lock)
      }
    }
    CarbonHiveIndexMetadataUtil.removeIndexInfoFromParentTable(
      parentCarbonTable.getIndexInfo,
      parentCarbonTable,
      dbName,
      indexName)(sparkSession)
    // clear parent table from meta store cache as it is also required to be
    // refreshed when SI table is dropped
    CarbonInternalMetastore
      .removeTableFromMetadataCache(dbName, parentTableName)(sparkSession)
  }
}
