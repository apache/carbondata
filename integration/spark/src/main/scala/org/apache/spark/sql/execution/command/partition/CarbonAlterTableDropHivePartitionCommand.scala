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

package org.apache.spark.sql.execution.command.partition

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AtomicRunnableCommand}
import org.apache.spark.sql.hive.CarbonHiveIndexMetadataUtil
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, CleanFilesUtil, TrashUtil}
import org.apache.carbondata.events._
import org.apache.carbondata.spark.rdd.CarbonDropPartitionRDD

/**
 * Drop the partitions from hive and carbon store. It drops the partitions in following steps
 * 1. Drop the partitions from carbon store, it just create one new mapper file in each segment
 * with unique id.
 * 2. Drop partitions from hive.
 * 3. In any above step fails then roll back the newly created files
 * 4. After success of steps 1 and 2 , it commits the files by removing the old fails.
 * Here it does not remove any data from store. During compaction the old data won't be considered.
 * @param tableName
 * @param specs
 * @param ifExists
 * @param purge
 * @param retainData
 */
case class CarbonAlterTableDropHivePartitionCommand(
    tableName: TableIdentifier,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    retainData: Boolean,
    operationContext: OperationContext = new OperationContext)
  extends AtomicRunnableCommand {

  var carbonPartitionsTobeDropped : util.List[String] = _
  var table: CarbonTable = _
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  lazy val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
    LockUsage.COMPACTION_LOCK,
    LockUsage.DELETE_SEGMENT_LOCK,
    LockUsage.DROP_TABLE_LOCK,
    LockUsage.CLEAN_FILES_LOCK,
    LockUsage.ALTER_PARTITION_LOCK)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
    setAuditTable(table)
    setAuditInfo(Map("partition" -> specs.mkString(",")))
    if (table.isHivePartitionTable) {
      var locks = List.empty[ICarbonLock]
      try {
        locks = AlterTableUtil.validateTableAndAcquireLock(
          table.getDatabaseName,
          table.getTableName,
          locksToBeAcquired)(sparkSession)
        val partitions =
          specs.flatMap(f => sparkSession.sessionState.catalog.listPartitions(tableName,
            Some(CarbonSparkSqlParserUtil.copyTablePartition(f))))
        val partitionLocations = partitions.map { partition =>
          FileFactory.getUpdatedFilePath(new Path(partition.location).toString)
        }
        carbonPartitionsTobeDropped = new util.ArrayList[String](partitionLocations.asJava)
        withEvents(operationContext,
          PreAlterTableHivePartitionCommandEvent(sparkSession, table),
          PostAlterTableHivePartitionCommandEvent(sparkSession, table)) {
          val metaEvent = AlterTableDropPartitionMetaEvent(table, specs, ifExists, purge,
            retainData, sparkSession)
          OperationListenerBus.getInstance()
            .fireEvent(metaEvent, operationContext)
          // Drop the partitions from hive.
          AlterTableDropPartitionCommand(
            tableName,
            specs,
            ifExists,
            purge,
            retainData).run(sparkSession)
          val isPartitionDataTrashEnabled = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_ENABLE_PARTITION_DATA_TRASH,
              CarbonCommonConstants.CARBON_ENABLE_PARTITION_DATA_TRASH_DEFAULT).toBoolean
          if (isPartitionDataTrashEnabled) {
            // move  the partition files to trash folder which are dropped
            val droppedPartitionNames = partitions.map { partition =>
              partition.spec.map { specs => specs._1 + CarbonCommonConstants.EQUALS + specs._2 }
            }
            val timeStamp = System.currentTimeMillis()
            droppedPartitionNames.zipWithIndex.foreach { partitionName =>
              val droppedPartitionName = droppedPartitionNames(partitionName._2).mkString("/")
              TrashUtil.copyPartitionDataToTrash(carbonPartitionsTobeDropped.get(partitionName._2),
                TrashUtil.getCompleteTrashFolderPathForPartition(
                  table.getTablePath,
                  timeStamp,
                  droppedPartitionName))
            }
            // Delete partition folder after copy to trash
            carbonPartitionsTobeDropped.asScala.foreach(delPartition => {
              val partitionPath = FileFactory.getCarbonFile(delPartition)
              CarbonUtil.deleteFoldersAndFiles(partitionPath)
            })
            // Finally delete empty partition folders.
            CleanFilesUtil.deleteEmptyPartitionFoldersRecursively(FileFactory
              .getCarbonFile(table.getTablePath))
          }
        }
      } catch {
        case e: Exception =>
          if (!ifExists) {
            throwMetadataException(table.getDatabaseName, table.getTableName, e.getMessage)
          } else {
            log.warn(e.getMessage)
            return Seq.empty[Row]
          }
      } finally {
        AlterTableUtil.releaseLocks(locks)
      }

    } else {
      throwMetadataException(tableName.database.getOrElse(sparkSession.catalog.currentDatabase),
        tableName.table,
        "Not a partitioned table")
    }
    Seq.empty[Row]
  }


  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    AlterTableAddPartitionCommand(tableName, specs.map((_, None)), true)
    val msg = s"Got exception $exception when processing data of drop partition." +
              "Adding back partitions to the metadata"
    LOGGER.error(msg)
    Seq.empty[Row]
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    var locks = List.empty[ICarbonLock]
    val uniqueId = System.currentTimeMillis().toString
    val tobeCleanSegs = new util.HashSet[String]
    try {
      locks = AlterTableUtil.validateTableAndAcquireLock(
        table.getDatabaseName,
        table.getTableName,
        locksToBeAcquired)(sparkSession)
      // If normal table then set uuid to ""
      val uuid = "";
      val segments = new SegmentStatusManager(table.getAbsoluteTableIdentifier,
        table.getTableStatusVersion).getValidAndInvalidSegments(table.isMV).getValidSegments
      // First drop the partitions from partition mapper files of each segment
      val tuples = new CarbonDropPartitionRDD(sparkSession,
        table.getTablePath,
        segments.asScala,
        carbonPartitionsTobeDropped,
        uniqueId).collect()
      val tobeUpdatedSegs = new util.ArrayList[String]
      val tobeDeletedSegs = new util.ArrayList[String]
      tuples.foreach{case (tobeUpdated, tobeDeleted) =>
        if (tobeUpdated.split(",").length > 0) {
          tobeUpdatedSegs.add(tobeUpdated.split(",")(0))
        }
        if (tobeDeleted.split(",").length > 0) {
          tobeDeletedSegs.add(tobeDeleted.split(",")(0))
        }
      }
      var tblStatusWriteVersion = ""
      withEvents(operationContext,
        AlterTableDropPartitionPreStatusEvent(table, sparkSession),
        AlterTableDropPartitionPostStatusEvent(table)) {
        tblStatusWriteVersion = SegmentFileStore.commitDropPartitions(table, uniqueId,
          tobeUpdatedSegs, tobeDeletedSegs, tblStatusWriteVersion)
      }
      CarbonHiveIndexMetadataUtil.updateTableStatusVersion(table,
        sparkSession, tblStatusWriteVersion)
      IndexStoreManager.getInstance().clearIndex(table.getAbsoluteTableIdentifier)
      tobeCleanSegs.addAll(tobeUpdatedSegs)
      tobeCleanSegs.addAll(tobeDeletedSegs)
    } finally {
      AlterTableUtil.releaseLocks(locks)
      SegmentFileStore.cleanSegments(table, tobeCleanSegs, null, true)
    }
    Seq.empty[Row]
  }

  override protected def opName: String = "DROP HIVE PARTITION"
}
