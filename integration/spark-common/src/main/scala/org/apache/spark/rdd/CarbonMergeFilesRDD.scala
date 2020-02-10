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

import java.util.concurrent.Executors

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
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
      partitionInfo: java.util.List[String] = new java.util.ArrayList[String](),
      tempFolderPath: String = null,
      readFileFooterFromCarbonDataFile: Boolean = false,
      currPartitionSpec: Option[String] = None
  ): Long = {
    var mergeIndexSize = 0L
    if (mergeIndexProperty) {
      new CarbonMergeFilesRDD(
        sparkSession,
        carbonTable,
        segmentIds,
        segmentFileNameToSegmentIdMap,
        carbonTable.isHivePartitionTable,
        readFileFooterFromCarbonDataFile,
        partitionInfo,
        tempFolderPath).collect()
    } else {
      try {
        if (isPropertySet(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT)) {
          val mergeFilesRDD = new CarbonMergeFilesRDD(
            sparkSession,
            carbonTable,
            segmentIds,
            segmentFileNameToSegmentIdMap,
            carbonTable.isHivePartitionTable,
            readFileFooterFromCarbonDataFile,
            partitionInfo,
            tempFolderPath,
            currPartitionSpec
          )
          if (carbonTable.isHivePartitionTable &&
              null != partitionInfo && !partitionInfo.isEmpty &&
              !StringUtils.isEmpty(tempFolderPath)) {
            // Async, distribute.
            val rows = mergeFilesRDD.collect()
            mergeIndexSize = rows.map(r => java.lang.Long.parseLong(r._1)).sum
            val segmentFiles = rows.map(_._2)
            if (segmentFiles.length > 0) {
              val finalSegmentFile = if (segmentFiles.length == 1) {
                segmentFiles(0)
              } else {
                val temp = segmentFiles(0)
                (1 until segmentFiles.length).foreach { index =>
                  temp.merge(segmentFiles(index))
                }
                temp
              }

              val segmentFilesLocation =
                CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath)
              val locationFile = FileFactory.getCarbonFile(segmentFilesLocation)
              if (!locationFile.exists()) {
                locationFile.mkdirs()
              }
              val segmentFilePath =
                CarbonTablePath
                  .getSegmentFilePath(carbonTable.getTablePath,
                    tempFolderPath.replace(".tmp", CarbonTablePath.SEGMENT_EXT))
              SegmentFileStore.writeSegmentFile(finalSegmentFile, segmentFilePath)
            }
          } else if (carbonTable.isHivePartitionTable && segmentIds.size > 1) {
            // Async, distribute.
            mergeFilesRDD.collect()
          } else {
            // Sync
            mergeFilesRDD.internalGetPartitions.foreach(
              partition => mergeFilesRDD.internalCompute(partition, null)
            )
          }
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
    if (carbonTable.isHivePartitionTable && !StringUtils.isEmpty(tempFolderPath)) {
      // remove all tmp folder of index files
      val startDelete = System.currentTimeMillis()
      val numThreads = Math.min(Math.max(partitionInfo.size(), 1), 10)
      val executorService = Executors.newFixedThreadPool(numThreads)
      val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
      partitionInfo
        .asScala
        .map { partitionPath =>
          executorService.submit(new Runnable {
            override def run(): Unit = {
              ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
              FileFactory.deleteAllCarbonFilesOfDir(
                FileFactory.getCarbonFile(partitionPath + "/" + tempFolderPath))
            }
          })
        }
        .map(_.get())
      LOGGER.info("Time taken to remove partition files for all partitions: " +
                  (System.currentTimeMillis() - startDelete))
    } else if (carbonTable.isHivePartitionTable) {
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
    mergeIndexSize
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
  readFileFooterFromCarbonDataFile: Boolean,
  partitionInfo: java.util.List[String],
  tempFolderPath: String,
  currPartitionSpec: Option[String] = None
) extends CarbonRDD[(String, SegmentFileStore.SegmentFile)](ss, Nil) {

  override def internalGetPartitions: Array[Partition] = {
    if (isHivePartitionedTable) {
      val metadataDetails = SegmentStatusManager
        .readLoadMetadata(CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
      // in case of partition table make rdd partitions per partition of the carbon table
      val partitionPaths: java.util.Map[String, java.util.List[String]] = new java.util.HashMap()
      if (partitionInfo == null || partitionInfo.isEmpty) {
        segments.foreach(segment => {
          val partitionSpecs = SegmentFileStore
            .getPartitionSpecs(segment, carbonTable.getTablePath, metadataDetails)
            .asScala.map(_.getLocation.toString)
          partitionPaths.put(segment, partitionSpecs.asJava)
        })
      } else {
        segments.foreach(segment => {
          partitionPaths.put(segment, partitionInfo)
        })
      }
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

  override def internalCompute(theSplit: Partition,
      context: TaskContext): Iterator[(String, SegmentFileStore.SegmentFile)] = {
    val tablePath = carbonTable.getTablePath
    val iter = new Iterator[(String, SegmentFileStore.SegmentFile)] {
      val split = theSplit.asInstanceOf[CarbonMergeFilePartition]
      logInfo("Merging carbon index files of segment : " +
              CarbonTablePath.getSegmentPath(tablePath, split.segmentId))

      var segmentFile: SegmentFileStore.SegmentFile = null
      var indexSize: String = ""
      if (isHivePartitionedTable && partitionInfo.isEmpty) {
        CarbonLoaderUtil.mergeIndexFilesInPartitionedSegment(
          carbonTable,
          split.segmentId,
          segmentFileNameToSegmentIdMap.get(split.segmentId),
          split.partitionPath)
      } else if (isHivePartitionedTable && !partitionInfo.isEmpty) {
        val folderDetails = CarbonLoaderUtil
          .mergeIndexFilesInPartitionedTempSegment(carbonTable,
            split.segmentId,
            split.partitionPath,
            partitionInfo,
            segmentFileNameToSegmentIdMap.get(split.segmentId),
            tempFolderPath,
            if (currPartitionSpec.isDefined) currPartitionSpec.get else null
          )

        val mergeIndexFilePath = split.partitionPath + "/" + folderDetails.getMergeFileName
        indexSize = "" + FileFactory.getCarbonFile(mergeIndexFilePath).getSize
        val locationKey = if (split.partitionPath.startsWith(carbonTable.getTablePath)) {
          split.partitionPath.substring(carbonTable.getTablePath.length)
        } else {
          split.partitionPath
        }
        segmentFile = SegmentFileStore.createSegmentFile(locationKey, folderDetails)
      } else {
        new CarbonIndexFileMergeWriter(carbonTable)
          .mergeCarbonIndexFilesOfSegment(split.segmentId,
            tablePath,
            readFileFooterFromCarbonDataFile,
            segmentFileNameToSegmentIdMap.get(split.segmentId))
      }

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (String, SegmentFileStore.SegmentFile) = {
        finished = true
        (indexSize, segmentFile)
      }

    }
    iter
  }

  override def getPartitions: Array[Partition] = {
    super
      .getPartitions
  }

}

