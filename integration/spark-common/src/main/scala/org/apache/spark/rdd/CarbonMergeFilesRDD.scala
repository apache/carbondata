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

package org.apache.spark.rdd

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.rdd.CarbonRDD

case class CarbonMergeFilePartition(rddId: Int,
    idx: Int,
    segmentId: String,
    partitionPath: String = null) extends Partition {

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

object CarbonMergeFilesRDD {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Merge the carbonindex files with in the segment to carbonindexmerge file inside same segment
   *
   * @param sparkSession carbon session
   * @param segmentIds the segments to process
   * @param segmentFileNameToSegmentIdMap a map that map the segmentFileName to segmentId
   * @param tablePath table path
   * @param carbonTable carbon table
   * @param mergeIndexProperty whether to merge the property of the carbon index, the usage
   *                           scenario is the same as that of `readFileFooterFromCarbonDataFile`
   * @param readFileFooterFromCarbonDataFile flag to read file footer information from carbondata
   *                                         file. This will used in case of upgrade from version
   *                                         which do not store the blocklet info to current
   *                                         version
   */
  def mergeIndexFiles(sparkSession: SparkSession,
      segmentIds: Seq[String],
      segmentFileNameToSegmentIdMap: java.util.Map[String, String],
      tablePath: String,
      carbonTable: CarbonTable,
      mergeIndexProperty: Boolean,
      readFileFooterFromCarbonDataFile: Boolean = false): Unit = {
    if (mergeIndexProperty) {
      new CarbonMergeFilesRDD(
        sparkSession,
        carbonTable,
        segmentIds,
        segmentFileNameToSegmentIdMap,
        carbonTable.isHivePartitionTable,
        readFileFooterFromCarbonDataFile).collect()
    } else {
      try {
        if (isPropertySet(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)) {
          new CarbonMergeFilesRDD(
            sparkSession,
            carbonTable,
            segmentIds,
            segmentFileNameToSegmentIdMap,
            carbonTable.isHivePartitionTable,
            readFileFooterFromCarbonDataFile).collect()
        }
      } catch {
        case ex: Exception =>
          val message = "Merge Index files request is failed " +
                        s"for table ${ carbonTable.getTableUniqueName }. " + ex.getMessage
          LOGGER.error(message)
          if (isPropertySet(CarbonCommonConstants.CARBON_MERGE_INDEX_FAILURE_THROW_EXCEPTION,
            CarbonCommonConstants.CARBON_MERGE_INDEX_FAILURE_THROW_EXCEPTION_DEFAULT)) {
            throw new RuntimeException(message, ex)
          }
      }
    }
    if (carbonTable.isHivePartitionTable) {
      segmentIds.foreach(segmentId => {
        val readPath: String = CarbonTablePath.getSegmentFilesLocation(tablePath) +
                               CarbonCommonConstants.FILE_SEPARATOR + segmentId + "_" +
                               segmentFileNameToSegmentIdMap.get(segmentId) + ".tmp"
        // Merge all partition files into a single file.
        val segmentFileName: String = SegmentFileStore
          .genSegmentFileName(segmentId, segmentFileNameToSegmentIdMap.get(segmentId))
        SegmentFileStore
          .mergeSegmentFiles(readPath,
            segmentFileName,
            CarbonTablePath.getSegmentFilesLocation(tablePath))
      })
    }
  }

  /**
   * Check whether the Merge Index Property is set by the user.
   * If not set, take the default value of the property.
   *
   * @return
   */
  def isPropertySet(property: String, defaultValue: String): Boolean = {
    var mergeIndex: Boolean = false
    try {
      mergeIndex = CarbonProperties.getInstance().getProperty(property, defaultValue).toBoolean
    } catch {
      case _: Exception =>
        mergeIndex = defaultValue.toBoolean
    }
    mergeIndex
  }
}

/**
 * RDD to merge all carbonindex files of each segment to carbonindex file into the same segment.
 * @param ss
 * @param carbonTable
 * @param segments segments to be merged
 */
class CarbonMergeFilesRDD(
  @transient private val ss: SparkSession,
  carbonTable: CarbonTable,
  segments: Seq[String],
  segmentFileNameToSegmentIdMap: java.util.Map[String, String],
  isHivePartitionedTable: Boolean,
  readFileFooterFromCarbonDataFile: Boolean)
  extends CarbonRDD[String](ss, Nil) {

  override def internalGetPartitions: Array[Partition] = {
    if (isHivePartitionedTable) {
      val metadataDetails = SegmentStatusManager
        .readLoadMetadata(CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
      // in case of partition table make rdd partitions per partition of the carbon table
      val partitionPaths: java.util.Map[String, java.util.List[String]] = new java.util.HashMap()
      segments.foreach(segment => {
        val partitionSpecs = SegmentFileStore
          .getPartitionSpecs(segment, carbonTable.getTablePath, metadataDetails)
          .asScala.map(_.getLocation.toString)
        partitionPaths.put(segment, partitionSpecs.asJava)
      })
      var index: Int = -1
      val rddPartitions: java.util.List[Partition] = new java.util.ArrayList()
      partitionPaths.asScala.foreach(partitionPath => {
        val segmentId = partitionPath._1
        partitionPath._2.asScala.map { partition =>
          index = index + 1
          rddPartitions.add(CarbonMergeFilePartition(id, index, segmentId, partition))
        }
      })
      rddPartitions.asScala.toArray
    } else {
      // in case of normal carbon table, make rdd partitions per segment
      segments.zipWithIndex.map { s =>
        CarbonMergeFilePartition(id, s._2, s._1)
      }.toArray
    }
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val tablePath = carbonTable.getTablePath
    val iter = new Iterator[String] {
      val split = theSplit.asInstanceOf[CarbonMergeFilePartition]
      logInfo("Merging carbon index files of segment : " +
              CarbonTablePath.getSegmentPath(tablePath, split.segmentId))

      if (isHivePartitionedTable) {
        CarbonLoaderUtil
          .mergeIndexFilesInPartitionedSegment(carbonTable, split.segmentId,
            segmentFileNameToSegmentIdMap.get(split.segmentId), split.partitionPath)
      } else {
        new CarbonIndexFileMergeWriter(carbonTable)
          .mergeCarbonIndexFilesOfSegment(split.segmentId,
            tablePath,
            readFileFooterFromCarbonDataFile,
            segmentFileNameToSegmentIdMap.get(split.segmentId))
      }

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = true
          havePair = !finished
        }
        !finished
      }

      override def next(): String = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        ""
      }

    }
    iter
  }

}

