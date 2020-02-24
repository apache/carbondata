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

package org.apache.spark.sql.execution.command.table

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonEnv, EnvHelper, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.datamap.CarbonDropDataMapCommand
import org.apache.spark.sql.hive.CarbonFileMetastore

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.events._

case class CarbonDropTableCommand(
    ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    dropChildTable: Boolean = false)
  extends AtomicRunnableCommand {

  var carbonTable: CarbonTable = _
  var childDropCommands : Seq[CarbonDropTableCommand] = Seq.empty
  var childDropDataMapCommands : Seq[CarbonDropDataMapCommand] = Seq.empty

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

    val dbName = databaseNameOp.getOrElse(sparkSession.catalog.currentDatabase)
    setAuditTable(dbName, tableName)
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
      carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
      val locksToBeAcquired: List[String] = if (carbonTable.isTransactionalTable) {
        List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
      } else {
        List.empty
      }
      val identifier = carbonTable.getAbsoluteTableIdentifier
      locksToBeAcquired foreach {
        lock => carbonLocks +=
                CarbonLockUtil.getLockObject(identifier, lock)
      }
      // check for directly drop datamap table
      if (carbonTable.isChildTableForMV && !dropChildTable) {
        if (!ifExistsSet) {
          throwMetadataException(dbName, tableName,
            "Child table which is associated with datamap cannot be dropped, " +
            "use DROP DATAMAP command to drop")
        } else {
          LOGGER.info("Skipping Drop table " + tableName +
                      " because Child table which is associated with datamap cannot be dropped")
          return Seq.empty
        }
      }

      if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
        throw new ConcurrentOperationException(carbonTable, "loading", "drop table")
      }
      if (carbonTable.isStreamingSink) {
        // streaming table should acquire streaming.lock
        carbonLocks += CarbonLockUtil.getLockObject(identifier, LockUsage.STREAMING_LOCK)
      }
      val operationContext = new OperationContext
      val dropTablePreEvent: DropTablePreEvent =
        DropTablePreEvent(
          carbonTable,
          ifExistsSet,
          sparkSession)
      OperationListenerBus.getInstance.fireEvent(dropTablePreEvent, operationContext)

      CarbonEnv.getInstance(sparkSession).carbonMetaStore.dropTable(identifier)(sparkSession)

      val dataMapSchemas = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
      LOGGER.info(s"Dropping DataMaps in table $tableName, size: ${dataMapSchemas.size()}")
      if (!dataMapSchemas.isEmpty) {
        childDropDataMapCommands = dataMapSchemas.asScala.map { schema =>
          val command = CarbonDropDataMapCommand(schema.getDataMapName,
            ifExistsSet,
            Some(TableIdentifier(tableName, Some(dbName))),
            forceDrop = true)
          command.dataMapSchema = schema
          command.mainTable = carbonTable
          command
        }
        childDropDataMapCommands.foreach(_.processMetadata(sparkSession))
      }

      // fires the event after dropping main table
      val dropTablePostEvent: DropTablePostEvent =
        DropTablePostEvent(
          carbonTable,
          ifExistsSet,
          sparkSession)
      OperationListenerBus.getInstance.fireEvent(dropTablePostEvent, operationContext)
      // Remove all invalid entries of carbonTable and corresponding updated timestamp
      // values from the cache. This case is valid when there are 2 JDBCServer and one of them
      // drops the table, the other server would not be able to clear its cache.
      try {
        CarbonEnv.getInstance(sparkSession).carbonMetaStore match {
          case metastore: CarbonFileMetastore => metastore.removeStaleTimeStampEntries(sparkSession)
          case _ =>
        }
      } catch {
        case _: Exception =>
        // Do nothing
      }
    } catch {
      case ex: NoSuchTableException =>
        if (!ifExistsSet) {
          throw ex
        } else {
          LOGGER.info("Masking error: " + ex.getLocalizedMessage)
        }
      case ex: ConcurrentOperationException =>
        LOGGER.error(ex.getLocalizedMessage, ex)
        throw ex
      case ex: Exception =>
        val msg = s"Dropping table $dbName.$tableName failed: ${ex.getMessage}"
        LOGGER.error(msg, ex)
        throwMetadataException(dbName, tableName, msg)
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

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // clear driver side index and dictionary cache
    if (!EnvHelper.isCloud(sparkSession)
        && carbonTable != null
        && !(carbonTable.isChildTableForMV && !dropChildTable)) {
      // delete the table folder
      val tablePath = carbonTable.getTablePath
      // delete table data only if it is not external table
      if (FileFactory.isFileExist(tablePath) &&
          !(carbonTable.isExternalTable || carbonTable.isFileLevelFormat)) {
        val file = FileFactory.getCarbonFile(tablePath)
        CarbonUtil.deleteFoldersAndFilesSilent(file)
      }
      // Delete lock directory if external lock path is specified.
      if (CarbonProperties.getInstance.getProperty(CarbonCommonConstants.LOCK_PATH,
        CarbonCommonConstants.LOCK_PATH_DEFAULT).toLowerCase
        .nonEmpty) {
        val tableLockPath = CarbonLockFactory
          .getLockpath(carbonTable.getCarbonTableIdentifier.getTableId)
        val file = FileFactory.getCarbonFile(tableLockPath)
        CarbonUtil.deleteFoldersAndFilesSilent(file)
      }
      childDropDataMapCommands.foreach(_.processData(sparkSession))
    }
    Seq.empty
  }

  override protected def opName: String = "DROP TABLE"
}
