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
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AtomicRunnableCommand}
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events._
import org.apache.carbondata.spark.rdd.CarbonDropPartitionRDD

/**
 * Drop the partitions from hive and carbon store. It drops the partitions in following steps
 * 1. Drop the partitions from carbon store, it just create one new mapper file in each segment
 * with uniqueid.
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

  var carbonPartitionsTobeDropped : util.List[PartitionSpec] = _
  var table: CarbonTable = _
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
    setAuditTable(table)
    setAuditInfo(Map("partition" -> specs.mkString(",")))
    if (table.isHivePartitionTable) {
      var locks = List.empty[ICarbonLock]
      try {
        val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
          LockUsage.COMPACTION_LOCK,
          LockUsage.DELETE_SEGMENT_LOCK,
          LockUsage.DROP_TABLE_LOCK,
          LockUsage.CLEAN_FILES_LOCK,
          LockUsage.ALTER_PARTITION_LOCK)
        locks = AlterTableUtil.validateTableAndAcquireLock(
          table.getDatabaseName,
          table.getTableName,
          locksToBeAcquired)(sparkSession)
        val partitions =
          specs.flatMap(f => sparkSession.sessionState.catalog.listPartitions(tableName,
            Some(CarbonSparkSqlParserUtil.copyTablePartition(f))))
        val carbonPartitions = partitions.map { partition =>
          new PartitionSpec(
            new util.ArrayList[String](
              partition.spec.seq.map { case (column, value) =>
                column.toLowerCase + "=" + value
              }.toList.asJava),
            partition.location)
        }
        carbonPartitionsTobeDropped = new util.ArrayList[PartitionSpec](carbonPartitions.asJava)
        val preAlterTableHivePartitionCommandEvent = PreAlterTableHivePartitionCommandEvent(
          sparkSession,
          table)
        OperationListenerBus.getInstance()
          .fireEvent(preAlterTableHivePartitionCommandEvent, operationContext)
        val metaEvent = AlterTableDropPartitionMetaEvent(table, specs, ifExists, purge, retainData)
        OperationListenerBus.getInstance()
          .fireEvent(metaEvent, operationContext)
        // Drop the partitions from hive.
        AlterTableDropPartitionCommand(
          tableName,
          specs,
          ifExists,
          purge,
          retainData).run(sparkSession)
        val postAlterTableHivePartitionCommandEvent = PostAlterTableHivePartitionCommandEvent(
          sparkSession,
          table)
        OperationListenerBus.getInstance()
          .fireEvent(postAlterTableHivePartitionCommandEvent, operationContext)
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
    try {
      val locksToBeAcquired = List(LockUsage.METADATA_LOCK,
        LockUsage.COMPACTION_LOCK,
        LockUsage.DELETE_SEGMENT_LOCK,
        LockUsage.DROP_TABLE_LOCK,
        LockUsage.CLEAN_FILES_LOCK,
        LockUsage.ALTER_PARTITION_LOCK)
      locks = AlterTableUtil.validateTableAndAcquireLock(
        table.getDatabaseName,
        table.getTableName,
        locksToBeAcquired)(sparkSession)
      // If normal table then set uuid to ""
      val uuid = "";
      val segments = new SegmentStatusManager(table.getAbsoluteTableIdentifier)
        .getValidAndInvalidSegments(table.isChildTableForMV).getValidSegments
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
      val preStatusEvent = AlterTableDropPartitionPreStatusEvent(table)
      OperationListenerBus.getInstance().fireEvent(preStatusEvent, operationContext)

      SegmentFileStore.commitDropPartitions(table, uniqueId, tobeUpdatedSegs, tobeDeletedSegs, uuid)

      val postStatusEvent = AlterTableDropPartitionPostStatusEvent(table)
      OperationListenerBus.getInstance().fireEvent(postStatusEvent, operationContext)

      DataMapStoreManager.getInstance().clearDataMaps(table.getAbsoluteTableIdentifier)
    } finally {
      AlterTableUtil.releaseLocks(locks)
      SegmentFileStore.cleanSegments(table, null, true)
    }
    Seq.empty[Row]
  }

  override protected def opName: String = "DROP HIVE PARTITION"
}
