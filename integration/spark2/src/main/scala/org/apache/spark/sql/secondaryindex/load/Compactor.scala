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

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.secondaryindex.command.{SecondaryIndex, SecondaryIndexModel}
import org.apache.spark.sql.secondaryindex.rdd.SecondaryIndexCreator
import org.apache.spark.sql.secondaryindex.util.{CarbonInternalScalaUtil, SecondaryIndexUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
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
    // get list from carbonTable.getIndexes method
    if (null == CarbonInternalScalaUtil.getIndexesMap(carbonMainTable)) {
      throw new Exception("Secondary index load failed")
    }
    val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
    indexTablesList.foreach { indexTableAndColumns =>
      val secondaryIndex = SecondaryIndex(Some(carbonLoadModel.getDatabaseName),
        carbonLoadModel.getTableName,
        indexTableAndColumns._2.asScala.toList,
        indexTableAndColumns._1)
      val secondaryIndexModel = SecondaryIndexModel(sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        validSegments,
        segmentIdToLoadStartTimeMapping)
      try {
        val segmentToSegmentTimestampMap: util.Map[String, String] = new java.util
        .HashMap[String, String]()
        val indexCarbonTable = SecondaryIndexCreator
          .createSecondaryIndex(secondaryIndexModel,
            segmentToSegmentTimestampMap, null,
            forceAccessSegment, isCompactionCall = true,
            isLoadToFailedSISegments = false)
        CarbonInternalLoaderUtil.updateLoadMetadataWithMergeStatus(
          indexCarbonTable,
          loadsToMerge,
          validSegments.head,
          carbonLoadModel,
          segmentToSegmentTimestampMap,
          segmentIdToLoadStartTimeMapping(validSegments.head),
          SegmentStatus.INSERT_IN_PROGRESS, 0L, List.empty.asJava)

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
            CarbonInternalScalaUtil
              .getCompressorForIndexTable(indexCarbonTable.getDatabaseName,
                indexCarbonTable.getTableName,
                carbonMainTable.getTableName)(sqlContext.sparkSession))

        // merge the data files of the compacted segments and take care of
        // merging the index files inside this if needed
        val rebuiltSegments = SecondaryIndexUtil.mergeDataFilesSISegments(
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          indexCarbonTable,
          loadMetadataDetails.toList.asJava, carbonLoadModelForMergeDataFiles)(sqlContext)

        CarbonInternalLoaderUtil.updateLoadMetadataWithMergeStatus(
          indexCarbonTable,
          loadsToMerge,
          validSegments.head,
          carbonLoadModel,
          segmentToSegmentTimestampMap,
          segmentIdToLoadStartTimeMapping(validSegments.head),
          SegmentStatus.SUCCESS,
          carbonLoadModelForMergeDataFiles.getFactTimeStamp, rebuiltSegments.toList.asJava)

      } catch {
        case ex: Exception =>
          LOGGER.error(s"Compaction failed for SI table ${secondaryIndex.indexTableName}", ex)
          throw ex
      }
    }
  }
}
