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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, AtomicRunnableCommand}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.PartitionMapFileStore
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.spark.rdd.{CarbonDropPartitionCommitRDD, CarbonDropPartitionRDD}

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
    retainData: Boolean)
  extends AtomicRunnableCommand {


  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
    if (table.isHivePartitionTable) {
      try {
        specs.flatMap(f => sparkSession.sessionState.catalog.listPartitions(tableName, Some(f)))
      } catch {
        case e: Exception =>
          if (!ifExists) {
            throw e
          } else {
            log.warn(e.getMessage)
            return Seq.empty[Row]
          }
      }

      // Drop the partitions from hive.
      AlterTableDropPartitionCommand(
        tableName,
        specs,
        ifExists,
        purge,
        retainData).run(sparkSession)
    }
    Seq.empty[Row]
  }


  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    AlterTableAddPartitionCommand(tableName, specs.map((_, None)), true)
    val msg = s"Got exception $exception when processing data of drop partition." +
              "Adding back partitions to the metadata"
    LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(msg)
    Seq.empty[Row]
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
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
      val partitionNames = specs.flatMap { f =>
        f.map(k => k._1 + "=" + k._2)
      }.toSet
      val segments = new SegmentStatusManager(table.getAbsoluteTableIdentifier)
        .getValidAndInvalidSegments.getValidSegments
      try {
        // First drop the partitions from partition mapper files of each segment
        new CarbonDropPartitionRDD(sparkSession.sparkContext,
          table.getTablePath,
          segments.asScala,
          partitionNames.toSeq,
          uniqueId,
          partialMatch = true).collect()
      } catch {
        case e: Exception =>
          // roll back the drop partitions from carbon store
          new CarbonDropPartitionCommitRDD(sparkSession.sparkContext,
            table.getTablePath,
            segments.asScala,
            false,
            uniqueId).collect()
          throw e
      }
      // commit the drop partitions from carbon store
      new CarbonDropPartitionCommitRDD(sparkSession.sparkContext,
        table.getTablePath,
        segments.asScala,
        true,
        uniqueId).collect()
      // Update the loadstatus with update time to clear cache from driver.
      val segmentSet = new util.HashSet[String](new SegmentStatusManager(table
        .getAbsoluteTableIdentifier).getValidAndInvalidSegments.getValidSegments)
      CarbonUpdateUtil.updateTableMetadataStatus(
        segmentSet,
        table,
        uniqueId,
        true,
        new util.ArrayList[String])
      DataMapStoreManager.getInstance().clearDataMaps(table.getAbsoluteTableIdentifier)
    } finally {
      AlterTableUtil.releaseLocks(locks)
      new PartitionMapFileStore().cleanSegments(
        table,
        new util.ArrayList(CarbonFilters.getPartitions(Seq.empty, sparkSession, tableName).asJava),
        false)
    }
    Seq.empty[Row]
  }

}
