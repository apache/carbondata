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

package org.apache.spark.sql.secondaryindex.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.index.CarbonIndexUtil

import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.util.CarbonLoaderUtil

/**
 * Utility Class for the Secondary Index creation flow
 */
object FileInternalUtil {

  def updateTableStatus(
    validSegments: List[String],
    databaseName: String,
    tableName: String,
    loadStatus: SegmentStatus,
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
    segmentToSegmentTimestampMap: java.util.Map[String, String],
    carbonTable: CarbonTable,
    sparkSession: SparkSession,
    newStartTime: Long = 0L,
    rebuiltSegments: Set[String] = Set.empty): Boolean = {
    var loadMetadataDetailsList = Array[LoadMetadataDetails]()
    val loadEndTime = CarbonUpdateUtil.readCurrentTime
    validSegments.foreach { segmentId =>
      val loadMetadataDetail = new LoadMetadataDetails
      loadMetadataDetail.setLoadName(segmentId)
      loadMetadataDetail.setSegmentStatus(loadStatus)
      if (rebuiltSegments.contains(loadMetadataDetail.getLoadName) && newStartTime != 0L) {
        loadMetadataDetail.setLoadStartTime(newStartTime)
      } else {
        loadMetadataDetail.setLoadStartTime(segmentIdToLoadStartTimeMapping(segmentId))
      }
      loadMetadataDetail.setLoadEndTime(loadEndTime)
      if (null != segmentToSegmentTimestampMap.get(segmentId)) {
        loadMetadataDetail
          .setSegmentFile(SegmentFileStore
                            .genSegmentFileName(segmentId,
                              segmentToSegmentTimestampMap.get(segmentId).toString) +
                          CarbonTablePath.SEGMENT_EXT)
      } else {
        loadMetadataDetail
          .setSegmentFile(SegmentFileStore
                            .genSegmentFileName(segmentId,
                              segmentIdToLoadStartTimeMapping(segmentId).toString) +
                          CarbonTablePath.SEGMENT_EXT)
      }
      CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(loadMetadataDetail, segmentId, carbonTable)
      loadMetadataDetailsList +:= loadMetadataDetail
    }
    val indexTables = CarbonIndexUtil
      .getIndexCarbonTables(carbonTable, sparkSession)
    val status = CarbonInternalLoaderUtil.recordLoadMetadata(
      loadMetadataDetailsList.toList.asJava,
      validSegments.asJava,
      carbonTable,
      indexTables.toList.asJava,
      databaseName,
      tableName
    )
    status
  }
}
