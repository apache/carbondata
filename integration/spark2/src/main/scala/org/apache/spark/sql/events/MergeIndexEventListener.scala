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

package org.apache.spark.sql.events

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events.{AlterTableCompactionPostEvent, AlterTableMergeIndexEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostExecutionEvent
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil
import org.apache.carbondata.spark.util.CommonUtil

class MergeIndexEventListener extends OperationEventListener with Logging {
  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case preStatusUpdateEvent: LoadTablePostExecutionEvent =>
        LOGGER.audit("Load post status event-listener called for merge index")
        val loadTablePreStatusUpdateEvent = event.asInstanceOf[LoadTablePostExecutionEvent]
        val carbonTableIdentifier = loadTablePreStatusUpdateEvent.getCarbonTableIdentifier
        val loadModel = loadTablePreStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = loadModel.getCarbonDataLoadSchema.getCarbonTable
        val compactedSegments = loadModel.getMergedSegmentIds
        val sparkSession = SparkSession.getActiveSession.get
        if(!carbonTable.isStreamingSink) {
          if (null != compactedSegments && !compactedSegments.isEmpty) {
            mergeIndexFilesForCompactedSegments(sparkSession.sparkContext,
              carbonTable,
              compactedSegments)
          } else {
            val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String,
              String]()

            segmentFileNameMap
              .put(loadModel.getSegmentId, String.valueOf(loadModel.getFactTimeStamp))
            CommonUtil.mergeIndexFiles(sparkSession.sparkContext,
              Seq(loadModel.getSegmentId),
              segmentFileNameMap,
              carbonTable.getTablePath,
              carbonTable, false)
            // clear Block dataMap Cache
            clearBlockDataMapCache(carbonTable, Seq(loadModel.getSegmentId))
          }
        }
      case alterTableCompactionPostEvent: AlterTableCompactionPostEvent =>
        LOGGER.audit("Merge index for compaction called")
        val alterTableCompactionPostEvent = event.asInstanceOf[AlterTableCompactionPostEvent]
        val carbonTable = alterTableCompactionPostEvent.carbonTable
        val mergedLoads = alterTableCompactionPostEvent.compactedLoads
        val sparkContext = alterTableCompactionPostEvent.sparkSession.sparkContext
        if(!carbonTable.isStreamingSink) {
          mergeIndexFilesForCompactedSegments(sparkContext, carbonTable, mergedLoads)
        }
      case alterTableMergeIndexEvent: AlterTableMergeIndexEvent =>
        val exceptionEvent = event.asInstanceOf[AlterTableMergeIndexEvent]
        val alterTableModel = exceptionEvent.alterTableModel
        val carbonMainTable = exceptionEvent.carbonTable
        val compactionType = alterTableModel.compactionType
        val sparkSession = exceptionEvent.sparkSession
        if (!carbonMainTable.isStreamingSink) {
          LOGGER.audit(s"Compaction request received for table " +
                       s"${ carbonMainTable.getDatabaseName }.${ carbonMainTable.getTableName }")
          LOGGER.info(s"Merge Index request received for table " +
                      s"${ carbonMainTable.getDatabaseName }.${ carbonMainTable.getTableName }")
          val lock = CarbonLockFactory.getCarbonLockObj(
            carbonMainTable.getAbsoluteTableIdentifier,
            LockUsage.COMPACTION_LOCK)

          try {
            if (lock.lockWithRetries()) {
              LOGGER.info("Acquired the compaction lock for table" +
                          s" ${ carbonMainTable.getDatabaseName }.${
                            carbonMainTable
                              .getTableName
                          }")
              val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
                carbonMainTable.getAbsoluteTableIdentifier).asScala
              val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
              validSegments.foreach { segment =>
                validSegmentIds += segment.getSegmentNo
              }
              val loadFolderDetailsArray = SegmentStatusManager
                .readLoadMetadata(carbonMainTable.getMetadataPath)
              val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String,
                String]()
              loadFolderDetailsArray.foreach(loadMetadataDetails => {
                segmentFileNameMap
                  .put(loadMetadataDetails.getLoadName,
                    String.valueOf(loadMetadataDetails.getLoadStartTime))
              })
              // in case of merge index file creation using Alter DDL command
              // readFileFooterFromCarbonDataFile flag should be true. This flag is check for legacy
              // store (store <= 1.1 version) and create merge Index file as per new store so that
              // old store is also upgraded to new store
              CommonUtil.mergeIndexFiles(
                sparkContext = sparkSession.sparkContext,
                segmentIds = validSegmentIds,
                segmentFileNameToSegmentIdMap = segmentFileNameMap,
                tablePath = carbonMainTable.getTablePath,
                carbonTable = carbonMainTable,
                mergeIndexProperty = true,
                readFileFooterFromCarbonDataFile = true)
              // clear Block dataMap Cache
              clearBlockDataMapCache(carbonMainTable, validSegmentIds)
              val requestMessage = "Compaction request completed for table "
              s"${ carbonMainTable.getDatabaseName }.${ carbonMainTable.getTableName }"
              LOGGER.audit(requestMessage)
              LOGGER.info(requestMessage)
            } else {
              val lockMessage = "Not able to acquire the compaction lock for table " +
                                s"${ carbonMainTable.getDatabaseName }.${
                                  carbonMainTable
                                    .getTableName
                                }"

              LOGGER.audit(lockMessage)
              LOGGER.error(lockMessage)
              CarbonException.analysisException(
                "Table is already locked for compaction. Please try after some time.")
            }
          } finally {
            lock.unlock()
          }
        }
    }
  }

  def mergeIndexFilesForCompactedSegments(sparkContext: SparkContext,
    carbonTable: CarbonTable,
    mergedLoads: util.List[String]): Unit = {
    // get only the valid segments of the table
    val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
      carbonTable.getAbsoluteTableIdentifier).asScala
    val mergedSegmentIds = new util.ArrayList[String]()
    mergedLoads.asScala.foreach(mergedLoad => {
      val loadName = mergedLoad
        .substring(mergedLoad.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                   CarbonCommonConstants.LOAD_FOLDER.length)
      mergedSegmentIds.add(loadName)
    })
    val loadFolderDetailsArray = SegmentStatusManager
      .readLoadMetadata(carbonTable.getMetadataPath)
    val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String, String]()
    loadFolderDetailsArray.foreach(loadMetadataDetails => {
      segmentFileNameMap
        .put(loadMetadataDetails.getLoadName, String.valueOf(loadMetadataDetails.getLoadStartTime))
    })
    // filter out only the valid segments from the list of compacted segments
    // Example: say compacted segments list contains 0.1, 3.1, 6.1, 0.2.
    // In this list 0.1, 3.1 and 6.1 are compacted to 0.2 in the level 2 compaction.
    // So, it is enough to do merge index only for 0.2 as it is the only valid segment in this list
    val validMergedSegIds = validSegments
      .filter { seg => mergedSegmentIds.contains(seg.getSegmentNo) }.map(_.getSegmentNo)
    if (null != validMergedSegIds && !validMergedSegIds.isEmpty) {
      CommonUtil.mergeIndexFiles(sparkContext,
          validMergedSegIds,
          segmentFileNameMap,
          carbonTable.getTablePath,
          carbonTable,
          false)
      // clear Block dataMap Cache
      clearBlockDataMapCache(carbonTable, validMergedSegIds)
    }
  }

  private def clearBlockDataMapCache(carbonTable: CarbonTable, segmentIds: Seq[String]): Unit = {
    // clear driver Block dataMap cache for each segment
    segmentIds.foreach { segmentId =>
      SegmentFileStore.clearBlockDataMapCache(carbonTable, segmentId)
    }
  }

}
