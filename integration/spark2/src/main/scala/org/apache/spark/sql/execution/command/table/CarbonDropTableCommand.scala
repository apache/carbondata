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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.AtomicRunnableCommand

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.dictionary.ManageDictionaryAndBTree
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events._
import org.apache.carbondata.spark.exception.{ConcurrentOperationException, ProcessMetaDataException}

case class CarbonDropTableCommand(
    ifExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    dropChildTable: Boolean = false)
  extends AtomicRunnableCommand {

  var carbonTable: CarbonTable = _
  var childDropCommands : Seq[CarbonDropTableCommand] = Seq.empty

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.DROP_TABLE_LOCK)
    val identifier = CarbonEnv.getIdentifier(databaseNameOp, tableName)(sparkSession)
    val dbName = identifier.getCarbonTableIdentifier.getDatabaseName
    val carbonLocks: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer()
    try {
      locksToBeAcquired foreach {
        lock => carbonLocks += CarbonLockUtil.getLockObject(identifier, lock)
      }
      carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
      if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
        throw new ConcurrentOperationException(carbonTable, "loading", "drop table")
      }
      LOGGER.audit(s"Deleting table [$tableName] under database [$dbName]")
      if (carbonTable.isStreamingTable) {
        // streaming table should acquire streaming.lock
        carbonLocks += CarbonLockUtil.getLockObject(identifier, LockUsage.STREAMING_LOCK)
      }
      val relationIdentifiers = carbonTable.getTableInfo.getParentRelationIdentifiers
      if (relationIdentifiers != null && !relationIdentifiers.isEmpty) {
        if (!dropChildTable) {
          if (!ifExistsSet) {
            throwMetadataException(dbName, tableName,
              "Child table which is associated with datamap cannot be dropped, " +
              "use DROP DATAMAP command to drop")
          } else {
            return Seq.empty
          }
        }
      }
      val operationContext = new OperationContext
      val dropTablePreEvent: DropTablePreEvent =
        DropTablePreEvent(
          carbonTable,
          ifExistsSet,
          sparkSession)
      OperationListenerBus.getInstance.fireEvent(dropTablePreEvent, operationContext)

      CarbonEnv.getInstance(sparkSession).carbonMetastore.dropTable(identifier)(sparkSession)

      if (carbonTable.hasDataMapSchema) {
        // drop all child tables
       val childSchemas = carbonTable.getTableInfo.getDataMapSchemaList

        childDropCommands = childSchemas.asScala
          .filter(_.getRelationIdentifier != null)
          .map { childSchema =>
            val childTable =
              CarbonEnv.getCarbonTable(
                TableIdentifier(childSchema.getRelationIdentifier.getTableName,
                  Some(childSchema.getRelationIdentifier.getDatabaseName)))(sparkSession)
            val dropCommand = CarbonDropTableCommand(
              ifExistsSet = true,
              Some(childSchema.getRelationIdentifier.getDatabaseName),
              childSchema.getRelationIdentifier.getTableName,
              dropChildTable = true
            )
            dropCommand.carbonTable = childTable
            dropCommand
          }
        childDropCommands.foreach(_.processMetadata(sparkSession))
        }

      // fires the event after dropping main table
      val dropTablePostEvent: DropTablePostEvent =
        DropTablePostEvent(
          carbonTable,
          ifExistsSet,
          sparkSession)
      OperationListenerBus.getInstance.fireEvent(dropTablePostEvent, operationContext)
      LOGGER.audit(s"Deleted table [$tableName] under database [$dbName]")

    } catch {
      case ex: NoSuchTableException =>
        if (!ifExistsSet) {
          throw ex
        }
      case ex: ConcurrentOperationException =>
        throw ex
      case ex: Exception =>
        val msg = s"Dropping table $dbName.$tableName failed: ${ex.getMessage}"
        LOGGER.error(ex, msg)
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
    if (carbonTable != null) {
      ManageDictionaryAndBTree.clearBTreeAndDictionaryLRUCache(carbonTable)
      // delete the table folder
      val tablePath = carbonTable.getTablePath
      val fileType = FileFactory.getFileType(tablePath)

      // delete table data only if it is not external table
      if (FileFactory.isFileExist(tablePath, fileType) &&
          !carbonTable.isExternalTable) {
        val file = FileFactory.getCarbonFile(tablePath, fileType)
        CarbonUtil.deleteFoldersAndFilesSilent(file)
      }
      if (carbonTable.hasDataMapSchema && childDropCommands.nonEmpty) {
        // drop all child tables
        childDropCommands.foreach(_.processData(sparkSession))
      }
    }
    Seq.empty
  }

}
