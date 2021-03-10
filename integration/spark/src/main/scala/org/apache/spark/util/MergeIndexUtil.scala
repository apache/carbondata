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

package org.apache.spark.util

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.CompactionCallableModel

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.ObjectSerializationUtil
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

object MergeIndexUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def mergeIndexFilesOnCompaction(compactionCallableModel: CompactionCallableModel): Unit = {
    val carbonTable = compactionCallableModel.carbonTable
    LOGGER.info(s"Merge index for compaction is called on table ${carbonTable.getTableUniqueName}")
    val mergedLoads = compactionCallableModel.compactedSegments
    val sparkSession = compactionCallableModel.sqlContext.sparkSession
    if (!carbonTable.isStreamingSink) {
      val mergedSegmentIds = getMergedSegmentIds(mergedLoads)
      val carbonLoadModel = compactionCallableModel.carbonLoadModel
      val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String, String]()
      var partitionInfo: java.util.List[String] = null
      var tempFolderPath: String = null
      var currPartitionSpec: Option[String] = None

      mergedSegmentIds.foreach{ mergedSegmentId =>
       segmentFileNameMap.put(mergedSegmentId, String.valueOf(carbonLoadModel.getFactTimeStamp))
      }

      if (carbonTable.isHivePartitionTable) {
        tempFolderPath = carbonLoadModel.getFactTimeStamp.toString + ".tmp"
        if (compactionCallableModel.compactedPartitions != null) {
          val partitionSpecs = compactionCallableModel.compactedPartitions.getOrElse(List.empty)
          currPartitionSpec = Some(ObjectSerializationUtil.convertObjectToString(
            new util.ArrayList(partitionSpecs.asJava)))
          partitionInfo = partitionSpecs.map(_.getLocation.toString).asJava
        }
      }

      CarbonMergeFilesRDD.mergeIndexFiles(sparkSession,
        mergedSegmentIds,
        segmentFileNameMap,
        carbonTable.getTablePath,
        carbonTable,
        false,
        partitionInfo,
        tempFolderPath,
        false,
        currPartitionSpec)
    }
  }

  private def getMergedSegmentIds(mergedLoads: util.List[String]) = {
    mergedLoads.asScala.map { mergedLoad =>
      mergedLoad.substring(mergedLoad.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                           CarbonCommonConstants.LOAD_FOLDER.length)
    }
  }

  def mergeIndexFilesForCompactedSegments(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      mergedLoads: util.List[String]): Unit = {
    // get only the valid segments of the table
    val validSegments = CarbonDataMergerUtil.getValidSegmentList(carbonTable).asScala
    val mergedSegmentIds = getMergedSegmentIds(mergedLoads)
    // filter out only the valid segments from the list of compacted segments
    // Example: say compacted segments list contains 0.1, 3.1, 6.1, 0.2.
    // In this list 0.1, 3.1 and 6.1 are compacted to 0.2 in the level 2 compaction.
    // So, it is enough to do merge index only for 0.2 as it is the only valid segment in this list
    val validMergedSegIds = validSegments
      .filter { seg => mergedSegmentIds.contains(seg.getSegmentNo) }.map(_.getSegmentNo)
    if (null != validMergedSegIds && validMergedSegIds.nonEmpty) {
      CarbonMergeFilesRDD.mergeIndexFiles(sparkSession,
        validMergedSegIds,
        SegmentStatusManager.mapSegmentToStartTime(carbonTable),
        carbonTable.getTablePath,
        carbonTable,
        false)
      // clear Block index Cache
      clearBlockIndexCache(carbonTable, validMergedSegIds)
    }
  }

  def clearBlockIndexCache(carbonTable: CarbonTable, segmentIds: Seq[String]): Unit = {
    // clear driver Block index cache for each segment
    segmentIds.foreach { segmentId =>
      SegmentFileStore.clearBlockIndexCache(carbonTable, segmentId)
    }
  }

}
