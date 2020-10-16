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
import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.MergeIndexUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatusManager}
import org.apache.carbondata.core.util.{DataLoadMetrics, ObjectSerializationUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

class MergeIndexEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case preStatusUpdateEvent: LoadTablePreStatusUpdateEvent =>
        LOGGER.info("Load post status event-listener called for merge index")
        val loadModel = preStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = loadModel.getCarbonDataLoadSchema.getCarbonTable
        val compactedSegments = loadModel.getMergedSegmentIds
        val sparkSession = SparkSession.getActiveSession.get
        var partitionInfo: util.List[String] = new util.ArrayList[String]()
        val partitionPath = operationContext.getProperty("partitionPath")
        if (partitionPath != null) {
          partitionInfo = ObjectSerializationUtil
            .convertStringToObject(partitionPath.asInstanceOf[String])
            .asInstanceOf[util.List[String]]
        }
        val tempPath = operationContext.getProperty("tempPath")
        val loadMetaDetails = loadModel.getCurrentLoadMetadataDetail
        if (loadMetaDetails != null && !loadMetaDetails.getFileFormat.equals(FileFormat.ROW_V1)) {
          if (null != compactedSegments && !compactedSegments.isEmpty) {
            MergeIndexUtil.mergeIndexFilesForCompactedSegments(sparkSession,
              carbonTable,
              compactedSegments)
          } else {
            val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String,
              String]()

            segmentFileNameMap
              .put(loadModel.getSegmentId, String.valueOf(loadModel.getFactTimeStamp))
            val startTime = System.currentTimeMillis()
            val currPartitionSpec = operationContext.getProperty("carbon.currentpartition")
            val currPartitionSpecOption: Option[String] = if (currPartitionSpec == null) {
              None
            } else {
              Option(currPartitionSpec.asInstanceOf[String])
            }
            val indexSize = CarbonMergeFilesRDD.mergeIndexFiles(sparkSession,
              Seq(loadModel.getSegmentId),
              segmentFileNameMap,
              carbonTable.getTablePath,
              carbonTable, false, partitionInfo,
              if (tempPath == null) {
                null
              } else {
                tempPath.toString
              },
              currPartitionSpec = currPartitionSpecOption
            )
            val metrics = new DataLoadMetrics
            metrics.setMergeIndexSize(indexSize)
            loadModel.setMetrics(metrics)
            LOGGER.info("Total time taken for merge index " +
                        (System.currentTimeMillis() - startTime))
            // clear Block index Cache
            MergeIndexUtil.clearBlockIndexCache(carbonTable, Seq(loadModel.getSegmentId))
          }
        }
      case alterTableMergeIndexEvent: AlterTableMergeIndexEvent =>
        val carbonMainTable = alterTableMergeIndexEvent.carbonTable
        val sparkSession = alterTableMergeIndexEvent.sparkSession
        LOGGER.info(s"Merge Index request received for table " +
                    s"${ carbonMainTable.getDatabaseName }.${ carbonMainTable.getTableName }")
        val lock = CarbonLockFactory.getCarbonLockObj(
          carbonMainTable.getAbsoluteTableIdentifier,
          LockUsage.COMPACTION_LOCK)

        try {
          if (lock.lockWithRetries()) {
            LOGGER.info("Acquired the compaction lock for table" +
                        s" ${ carbonMainTable.getDatabaseName }.${ carbonMainTable.getTableName}")
            val loadFolderDetailsArray = SegmentStatusManager
              .readLoadMetadata(carbonMainTable.getMetadataPath)
            val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String,
              String]()
            var streamingSegment: Set[String] = Set[String]()
            loadFolderDetailsArray.foreach(loadMetadataDetails => {
              if (loadMetadataDetails.getFileFormat.equals(FileFormat.ROW_V1)) {
                streamingSegment += loadMetadataDetails.getLoadName
              }
              segmentFileNameMap
                .put(loadMetadataDetails.getLoadName,
                  String.valueOf(loadMetadataDetails.getLoadStartTime))
            })
            val validSegments =
              CarbonDataMergerUtil.getValidSegmentList(carbonMainTable).asScala
            var segmentsToMerge =
              if (alterTableMergeIndexEvent.alterTableModel.customSegmentIds.isEmpty) {
                val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
                validSegments.foreach { segment =>
                  // do not add ROW_V1 format
                  if (!segment.getLoadMetadataDetails.getFileFormat.equals(FileFormat.ROW_V1)) {
                    validSegmentIds += segment.getSegmentNo
                  }
                }
                validSegmentIds
              } else {
                alterTableMergeIndexEvent.alterTableModel
                  .customSegmentIds
                  .get
                  .filterNot(streamingSegment.contains(_))
              }
            validSegments.filter(x => segmentsToMerge.contains(x.getSegmentNo)).foreach { segment =>
              val segmentFile = segment.getSegmentFileName
              val sfs = new SegmentFileStore(carbonMainTable.getTablePath, segmentFile)
              if (sfs.getSegmentFile != null) {
                val indexFiles = sfs.getIndexCarbonFiles
                val segmentPath = CarbonTablePath
                  .getSegmentPath(carbonMainTable.getTablePath, segment.getSegmentNo)
                if (indexFiles.size() == 0) {
                  LOGGER.warn(s"No index files present in path: $segmentPath to merge")
                  // call merge if segments have index files
                  segmentsToMerge = segmentsToMerge.toStream
                    .filterNot(s => s.equals(segment.getSegmentNo)).toList
                }
              }
            }
            // in case of merge index file creation using Alter DDL command
            // readFileFooterFromCarbonDataFile flag should be true. This flag is check for legacy
            // store (store <= 1.1 version) and create merge Index file as per new store so that
            // old store is also upgraded to new store
            val startTime = System.currentTimeMillis()
            val partitionInfo: util.List[String] = operationContext
              .getProperty("partitionPath")
              .asInstanceOf[util.List[String]]
            val currPartitionSpec = operationContext.getProperty("carbon.currentpartition")
            val currPartitionSpecOption: Option[String] = if (currPartitionSpec == null) {
              None
            } else {
              Option(currPartitionSpec.asInstanceOf[String])
            }
            CarbonMergeFilesRDD.mergeIndexFiles(
              sparkSession = sparkSession,
              segmentIds = segmentsToMerge,
              segmentFileNameToSegmentIdMap = segmentFileNameMap,
              tablePath = carbonMainTable.getTablePath,
              carbonTable = carbonMainTable,
              mergeIndexProperty = true,
              readFileFooterFromCarbonDataFile = true,
              partitionInfo = partitionInfo,
              currPartitionSpec = currPartitionSpecOption)
            LOGGER.info("Total time taken for merge index "
                        + (System.currentTimeMillis() - startTime) + "ms")
            // clear Block index Cache
            MergeIndexUtil.clearBlockIndexCache(carbonMainTable, segmentsToMerge)
            val requestMessage = "Compaction request completed for table " +
              s"${ carbonMainTable.getDatabaseName }.${ carbonMainTable.getTableName }"
            LOGGER.info(requestMessage)
          } else {
            val lockMessage = "Not able to acquire the compaction lock for table " +
                              s"${ carbonMainTable.getDatabaseName }." +
                              s"${ carbonMainTable.getTableName}"
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
