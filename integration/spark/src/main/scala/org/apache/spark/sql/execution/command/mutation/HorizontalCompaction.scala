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

package org.apache.spark.sql.execution.command.mutation

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.AlterTableModel
import org.apache.spark.sql.execution.command.management.CarbonAlterTableCompactionCommand
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CarbonDataMergerUtilResult, CompactionType}

object HorizontalCompaction {

  val LOG = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * The method does horizontal compaction. After Update and Delete completion
   * tryHorizontal compaction will be called. In case this method is called after
   * Update statement then Update Compaction followed by Delete Compaction will be
   * processed whereas for tryHorizontalCompaction called after Delete statement
   * then only Delete Compaction will be processed.
   */
  def tryHorizontalCompaction(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      isUpdateOperation: Boolean): Unit = {

    if (!CarbonDataMergerUtil.isHorizontalCompactionEnabled) {
      return
    }

    var compactionTypeIUD = CompactionType.IUD_UPDDEL_DELTA
    val absTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val updateTimeStamp = System.currentTimeMillis()
    // To make sure that update and delete timestamps are not same,
    // required to commit to status metadata and cleanup
    val deleteTimeStamp = updateTimeStamp + 1

    // get the valid segments
    var segLists = CarbonDataMergerUtil.getValidSegmentList(carbonTable)

    if (segLists == null || segLists.size() == 0) {
      return
    }

    // Should avoid reading Table Status file from Disk every time. Better to load it
    // in-memory at the starting and pass it along the routines. The constructor of
    // SegmentUpdateStatusManager reads the Table Status File and Table Update Status
    // file and save the content in segmentDetails and updateDetails respectively.
    val segmentUpdateStatusManager: SegmentUpdateStatusManager = new SegmentUpdateStatusManager(
      carbonTable)

    if (isUpdateOperation) {

      // This is only update operation, perform only update compaction.
      compactionTypeIUD = CompactionType.IUD_UPDDEL_DELTA
      performUpdateDeltaCompaction(sparkSession,
        compactionTypeIUD,
        carbonTable,
        absTableIdentifier,
        segmentUpdateStatusManager,
        updateTimeStamp,
        segLists)
    }

    // After Update Compaction perform delete compaction
    compactionTypeIUD = CompactionType.IUD_DELETE_DELTA
    segLists = CarbonDataMergerUtil.getValidSegmentList(carbonTable)
    if (segLists == null || segLists.size() == 0) {
      return
    }

    // Delete Compaction
    performDeleteDeltaCompaction(sparkSession,
      compactionTypeIUD,
      carbonTable,
      absTableIdentifier,
      segmentUpdateStatusManager,
      deleteTimeStamp,
      segLists)

    // If there are already index and data files are present for old update operation, then the
    // cache will be loaded for those files during current update, but once after horizontal
    // compaction is finished, new compacted files are generated, so the segments inside cache are
    // now invalid, so clear the cache of invalid segment after horizontal compaction.
    DataMapStoreManager.getInstance()
      .clearInvalidSegments(carbonTable, segLists.asScala.map(_.getSegmentNo).asJava)
  }

  /**
   * Update Delta Horizontal Compaction.
   */
  private def performUpdateDeltaCompaction(sparkSession: SparkSession,
      compactionTypeIUD: CompactionType,
      carbonTable: CarbonTable,
      absTableIdentifier: AbsoluteTableIdentifier,
      segmentUpdateStatusManager: SegmentUpdateStatusManager,
      factTimeStamp: Long,
      segLists: util.List[Segment]): Unit = {
    val db = carbonTable.getDatabaseName
    val table = carbonTable.getTableName
    // get the valid segments qualified for update compaction.
    val validSegList = CarbonDataMergerUtil.getSegListIUDCompactionQualified(segLists,
      absTableIdentifier,
      segmentUpdateStatusManager,
      compactionTypeIUD)

    if (validSegList.size() == 0) {
      return
    }

    LOG.info(s"Horizontal Update Compaction operation started for [$db.$table].")

    try {
      // Update Compaction.
      val alterTableModel = AlterTableModel(Option(carbonTable.getDatabaseName),
        carbonTable.getTableName,
        Some(segmentUpdateStatusManager),
        CompactionType.IUD_UPDDEL_DELTA.toString,
        Some(factTimeStamp),
        "")

      CarbonAlterTableCompactionCommand(alterTableModel).run(sparkSession)
    }
    catch {
      case e: Exception =>
        val msg = if (null != e.getMessage) {
          e.getMessage
        } else {
          "Please check logs for more info"
        }
        throw new HorizontalCompactionException(
          s"Horizontal Update Compaction Failed for [${ db }.${ table }]. " + msg, factTimeStamp)
    }
    LOG.info(s"Horizontal Update Compaction operation completed for [${ db }.${ table }].")
  }

  /**
   * Delete Delta Horizontal Compaction.
   */
  private def performDeleteDeltaCompaction(sparkSession: SparkSession,
      compactionTypeIUD: CompactionType,
      carbonTable: CarbonTable,
      absTableIdentifier: AbsoluteTableIdentifier,
      segmentUpdateStatusManager: SegmentUpdateStatusManager,
      factTimeStamp: Long,
      segLists: util.List[Segment]): Unit = {

    val db = carbonTable.getDatabaseName
    val table = carbonTable.getTableName
    val deletedBlocksList = CarbonDataMergerUtil.getSegListIUDCompactionQualified(segLists,
      absTableIdentifier,
      segmentUpdateStatusManager,
      compactionTypeIUD)

    if (deletedBlocksList.size() == 0) {
      return
    }

    LOG.info(s"Horizontal Delete Compaction operation started for [$db.$table].")

    try {

      // Delete Compaction RDD
      val rdd1 = sparkSession.sparkContext
        .parallelize(deletedBlocksList.asScala, deletedBlocksList.size())

      val timestamp = factTimeStamp
      val updateStatusDetails = segmentUpdateStatusManager.getUpdateStatusDetails
      val conf = SparkSQLUtil
        .broadCastHadoopConf(sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())
      val result = rdd1.mapPartitions(iter =>
        new Iterator[Seq[CarbonDataMergerUtilResult]] {
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
          override def hasNext: Boolean = iter.hasNext

          override def next(): Seq[CarbonDataMergerUtilResult] = {
            val segmentAndBlocks = iter.next
            val segment = segmentAndBlocks.substring(0, segmentAndBlocks.lastIndexOf("/"))
            val blockName = segmentAndBlocks
              .substring(segmentAndBlocks.lastIndexOf("/") + 1, segmentAndBlocks.length)

            val result = CarbonDataMergerUtil.compactBlockDeleteDeltaFiles(segment, blockName,
              carbonTable,
              updateStatusDetails,
              timestamp)

            result.asScala.toList

          }
        }).collect

      val resultList = ListBuffer[CarbonDataMergerUtilResult]()
      result.foreach(x => {
        x.foreach(y => {
          resultList += y
        })
      })

      val updateStatus = CarbonDataMergerUtil.updateStatusFile(resultList.toList.asJava,
        carbonTable,
        timestamp.toString,
        segmentUpdateStatusManager)
      if (updateStatus == false) {
        LOG.error("Delete Compaction data operation is failed.")
        throw new HorizontalCompactionException(
          s"Horizontal Delete Compaction Failed for [$db.$table] ." +
          s" Please check logs for more info.", factTimeStamp)
      }
      else {
        LOG.info(s"Horizontal Delete Compaction operation completed for [$db.$table].")
      }
    }
    catch {
      case e: Exception =>
        val msg = if (null != e.getMessage) {
          e.getMessage
        } else {
          "Please check logs for more info"
        }
        throw new HorizontalCompactionException(
          s"Horizontal Delete Compaction Failed for [${ db }.${ table }]. " + msg, factTimeStamp)
    }
  }
}

