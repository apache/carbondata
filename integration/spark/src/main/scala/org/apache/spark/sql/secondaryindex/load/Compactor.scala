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
package org.apache.spark.sql.secondaryindex.load

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.{CarbonEnv, SQLContext}
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.command.{IndexModel, SecondaryIndexModel}
import org.apache.spark.sql.secondaryindex.events.LoadTableSIPostExecutionEvent
import org.apache.spark.sql.secondaryindex.rdd.SecondaryIndexCreator
import org.apache.spark.sql.secondaryindex.util.SecondaryIndexUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.status.IndexStatus
import org.apache.carbondata.core.locks.ICarbonLock
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.events.OperationListenerBus
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

object Compactor {

  /**
   * This method will create secondary index for all the index tables after compaction is completed
   *
   */
  def createSecondaryIndexAfterCompaction(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      validSegments: scala.List[String],
      loadsToMerge: Array[String],
      segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
      forceAccessSegment: Boolean = false): Unit = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var siCompactionIndexList = scala.collection.immutable.List[CarbonTable]()
    val sparkSession = sqlContext.sparkSession
    // get list from carbonTable.getIndexes method
    val indexProviderMap = carbonMainTable.getIndexesMap
    if (null == indexProviderMap) {
      throw new Exception("Secondary index load failed")
    }
    val iterator = if (null != indexProviderMap.get(IndexType.SI.getIndexProviderName)) {
      indexProviderMap.get(IndexType.SI.getIndexProviderName).entrySet().iterator()
    } else {
      java.util.Collections.emptyIterator()
    }
    var allSegmentsLock: scala.collection.mutable.ListBuffer[ICarbonLock] = ListBuffer.empty
    while (iterator.hasNext) {
      val index = iterator.next()
      val indexColumns = index.getValue.get(CarbonCommonConstants.INDEX_COLUMNS).split(",").toList
      val secondaryIndex = IndexModel(Some(carbonLoadModel.getDatabaseName),
        carbonLoadModel.getTableName,
        indexColumns,
        index.getKey)
      val indexCarbonTable = CarbonEnv.getCarbonTable(Some(carbonLoadModel.getDatabaseName),
        index.getKey)(sqlContext.sparkSession)
      val header = indexCarbonTable.getCreateOrderColumn.asScala
        .map(_.getColName).toArray
      CarbonIndexUtil.initializeSILoadModel(carbonLoadModel, header)
      val secondaryIndexModel = SecondaryIndexModel(sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        validSegments,
        segmentIdToLoadStartTimeMapping)
      try {
        val segmentToSegmentTimestampMap: util.Map[String, String] = new java.util
        .HashMap[String, String]()
        val (indexCarbonTable, segmentLocks, operationContext) = SecondaryIndexCreator
          .createSecondaryIndex(secondaryIndexModel,
            segmentToSegmentTimestampMap, null,
            forceAccessSegment, isCompactionCall = true,
            isLoadToFailedSISegments = false)
        if (segmentLocks.isEmpty) {
          LOGGER.error(s"Not able to acquire segment lock on the specific segment. " +
            s"Load the compacted segment ${validSegments.head} into SI table failed");
          return
        }
        allSegmentsLock ++= segmentLocks

        // merge index files
        CarbonMergeFilesRDD.mergeIndexFiles(sqlContext.sparkSession,
          secondaryIndexModel.validSegments,
          segmentToSegmentTimestampMap,
          indexCarbonTable.getTablePath,
          indexCarbonTable, mergeIndexProperty = false)

        val loadMetadataDetails = SegmentStatusManager
          .readLoadMetadata(indexCarbonTable.getMetadataPath)
          .filter(loadMetadataDetail => validSegments.head
            .equalsIgnoreCase(loadMetadataDetail.getLoadName))

        val carbonLoadModelForMergeDataFiles = SecondaryIndexUtil
          .getCarbonLoadModel(indexCarbonTable,
            loadMetadataDetails.toList.asJava,
            System.currentTimeMillis(),
            CarbonIndexUtil.getCompressorForIndexTable(indexCarbonTable, carbonMainTable))

        // merge the data files of the compacted segments and take care of
        // merging the index files inside this if needed
        val rebuiltSegments = SecondaryIndexUtil.mergeDataFilesSISegments(
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          indexCarbonTable,
          loadMetadataDetails.toList.asJava, carbonLoadModelForMergeDataFiles)(sqlContext)
        if (rebuiltSegments.isEmpty) {
          for (eachSegment <- secondaryIndexModel.validSegments) {
            SegmentFileStore
              .writeSegmentFile(indexCarbonTable,
                eachSegment,
                String.valueOf(carbonLoadModel.getFactTimeStamp))
          }
        }
        CarbonInternalLoaderUtil.updateLoadMetadataWithMergeStatus(
          indexCarbonTable,
          loadsToMerge,
          validSegments.head,
          segmentToSegmentTimestampMap,
          segmentIdToLoadStartTimeMapping(validSegments.head),
          SegmentStatus.SUCCESS,
          carbonLoadModelForMergeDataFiles.getFactTimeStamp, rebuiltSegments.toList.asJava)

        // Index PrePriming for SI
        DistributedRDDUtils.triggerPrepriming(sparkSession, indexCarbonTable, Seq(),
          operationContext, FileFactory.getConfiguration, validSegments)

        val loadTableSIPostExecutionEvent: LoadTableSIPostExecutionEvent =
          LoadTableSIPostExecutionEvent(sparkSession,
            indexCarbonTable.getCarbonTableIdentifier,
            secondaryIndexModel.carbonLoadModel,
            indexCarbonTable)
        OperationListenerBus.getInstance
          .fireEvent(loadTableSIPostExecutionEvent, operationContext)

        siCompactionIndexList ::= indexCarbonTable
      } catch {
        case ex: Exception =>
          LOGGER.error(s"Compaction failed for SI table ${secondaryIndex.indexName}", ex)
          throw ex
      } finally {
        // once compaction is success, release the segment locks
        allSegmentsLock.foreach { segmentLock =>
          segmentLock.unlock()
        }
      }
    }
  }
}
