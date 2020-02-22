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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.table.CarbonDropTableCommand

import org.apache.carbondata.common.exceptions.sql.NoSuchIndexException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapProvider, DataMapStoreManager}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.datamap.{DataMapManager, IndexProvider}
import org.apache.carbondata.events._

/**
 * Drops the index and any related tables associated with the index
 * @param indexName
 * @param ifExistsSet
 * @param table
 */
case class CarbonDropIndexCommand(
    indexName: String,
    ifExistsSet: Boolean,
    table: TableIdentifier,
    forceDrop: Boolean = false)
  extends AtomicRunnableCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  private var dataMapProvider: DataMapProvider = _
  var mainTable: CarbonTable = _
  var dataMapSchema: DataMapSchema = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    setAuditInfo(Map("indexName" -> indexName))
    val databaseNameOp = table.database
    val tableName = table.table
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK)
    if (mainTable == null) {
      try {
        mainTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
      } catch {
        case ex: NoSuchTableException =>
          throwMetadataException(dbName, tableName,
            s"Dropping index $indexName failed: ${ ex.getMessage }")
      }
    }
    setAuditTable(mainTable)
    val tableIdentifier = AbsoluteTableIdentifier.from(
      mainTable.getTablePath,
      dbName.toLowerCase,
      tableName.toLowerCase,
      mainTable.getCarbonTableIdentifier.getTableId)
    // forceDrop will be true only when parent table schema updation has failed.
    // This method will forcefully drop child table instance from metastore.
    if (forceDrop) {
      val childTableName = tableName + "_" + indexName
      LOGGER.info(s"Trying to force drop $childTableName from metastore")
      val childCarbonTable: Option[CarbonTable] = try {
        Some(CarbonEnv.getCarbonTable(databaseNameOp, childTableName)(sparkSession))
      } catch {
        case _: Exception =>
          LOGGER.warn(s"Child table $childTableName not found in metastore")
          None
      }
      if (childCarbonTable.isDefined) {
        val commandToRun = CarbonDropTableCommand(
          ifExistsSet = true,
          Some(childCarbonTable.get.getDatabaseName),
          childCarbonTable.get.getTableName,
          dropChildTable = true)
        commandToRun.run(sparkSession)
      }
      dropSchemaFromSystemFolder(sparkSession)
      return Seq.empty
    }
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
      locksToBeAcquired foreach {
        lock => carbonLocks += CarbonLockUtil.getLockObject(tableIdentifier, lock)
      }
      // drop index on the main table.
      val schema = DataMapStoreManager.getInstance().getIndexSchema(indexName)
      if (schema != null && schema.isIndexDataMap) {
        dropSchemaFromSystemFolder(sparkSession)
        return Seq.empty
      }
    } catch {
      case e: NoSuchIndexException =>
        if (!ifExistsSet) {
          throw e
        }
      case ex: Exception =>
        LOGGER.error(s"Dropping index $indexName failed", ex)
        throwMetadataException(dbName, tableName,
          s"Dropping index $indexName failed: ${ ex.getMessage }")
    } finally {
      if (carbonLocks.nonEmpty) {
        val unlocked = carbonLocks.forall(_.unlock())
        if (unlocked) {
          LOGGER.info("Table MetaData Unlocked Successfully")
        }
      }
    }

    Seq.empty
  }

  private def dropSchemaFromSystemFolder(sparkSession: SparkSession): Unit = {
    LOGGER.info("Trying to drop Index from system folder")
    try {
      if (dataMapSchema == null) {
        dataMapSchema = DataMapStoreManager.getInstance().getDataMapSchema(indexName)
      }
      if (dataMapSchema != null) {
        dataMapProvider =
          DataMapManager.get.getDataMapProvider(mainTable, dataMapSchema, sparkSession)
        val operationContext: OperationContext = new OperationContext()
        val systemFolderLocation: String = CarbonProperties.getInstance().getSystemFolderLocation
        val updateDataMapPreExecutionEvent: UpdateDataMapPreExecutionEvent =
          UpdateDataMapPreExecutionEvent(sparkSession, systemFolderLocation, null)
        OperationListenerBus.getInstance().fireEvent(updateDataMapPreExecutionEvent,
          operationContext)
        DataMapStatusManager.dropDataMap(dataMapSchema.getDataMapName)
        val updateDataMapPostExecutionEvent: UpdateDataMapPostExecutionEvent =
          UpdateDataMapPostExecutionEvent(sparkSession, systemFolderLocation, null)
        OperationListenerBus.getInstance().fireEvent(updateDataMapPostExecutionEvent,
          operationContext)
        // if it is indexDataMap provider like lucene, then call cleanData, which will launch a job
        // to clear index from memory(clears from segmentMap and cache), This is called before
        // deleting the index schemas from _System folder
        if (dataMapProvider.isInstanceOf[IndexProvider]) {
          dataMapProvider.cleanData()
        }
        dataMapProvider.cleanMeta()
      }
    } catch {
      case e: Exception =>
        if (!ifExistsSet) {
          throw e
        }
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // delete the table folder
    if (dataMapProvider != null) {
      dataMapProvider.cleanData()
    }
    Seq.empty
  }

  override protected def opName: String = "DROP INDEX"
}
