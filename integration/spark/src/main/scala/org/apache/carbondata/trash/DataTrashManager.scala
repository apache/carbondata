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

package org.apache.carbondata.trash

import scala.collection.JavaConverters._

import org.apache.commons.lang.StringUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, CleanFilesUtil, DeleteLoadFolders, TrashUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath

object DataTrashManager {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * clean garbage data
   *  1. check and clean .Trash folder
   *  2. move stale segments without metadata into .Trash
   *  3. clean expired segments(MARKED_FOR_DELETE, Compacted, In Progress)
   *
   * @param isForceDelete        clean the MFD/Compacted segments immediately and empty trash folder
   * @param cleanStaleInProgress clean the In Progress segments based on retention time,
   *                             it will clean immediately when force is true
   */
  def cleanGarbageData(
      carbonTable: CarbonTable,
      isForceDelete: Boolean,
      cleanStaleInProgress: Boolean,
      showStatistics: Boolean,
      partitionSpecs: Option[Seq[PartitionSpec]] = None) : Long = {
    // if isForceDelete = true need to throw exception if CARBON_CLEAN_FILES_FORCE_ALLOWED is false
    if (isForceDelete && !CarbonProperties.getInstance().isCleanFilesForceAllowed) {
      LOGGER.error("Clean Files with Force option deletes the physical data and it cannot be" +
        " recovered. It is disabled by default, to enable clean files with force option," +
        " set " + CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED + " to true")
      throw new RuntimeException("Clean files with force operation not permitted by default")
    }
    var carbonCleanFilesLock: ICarbonLock = null
    var carbonDeleteSegmentLock : ICarbonLock = null
    try {
      val errorMsg = "Clean files request is failed for " +
        s"${ carbonTable.getQualifiedName }" +
        ". Not able to acquire the clean files lock due to another clean files " +
        "operation is running in the background."
      val deleteSegmentErrorMsg = "Clean files request is failed for " +
        s"${ carbonTable.getQualifiedName }" +
        ". Not able to acquire the delete segment lock due to another delete segment " +
        "operation running in the background."
      carbonCleanFilesLock = CarbonLockUtil.getLockObject(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.CLEAN_FILES_LOCK, errorMsg)
      carbonDeleteSegmentLock = CarbonLockUtil.getLockObject(carbonTable
        .getAbsoluteTableIdentifier, LockUsage.DELETE_SEGMENT_LOCK, deleteSegmentErrorMsg)
      // step 1: check and clean trash folder
      // trashFolderSizeStats(0) contains the size that is freed/or can be freed and
      // trashFolderSizeStats(1) contains the size of remaining data in the trash folder
      val trashFolderSizeStats = checkAndCleanTrashFolder(carbonTable, isForceDelete,
          isDryRun = false, showStatistics)
      // step 2: move stale segments which are not exists in metadata into .Trash
      moveStaleSegmentsToTrash(carbonTable)
      // clean all the stale delete delta files, which are generated as the part of
      // horizontal compaction
      val deltaFileSize = if (isForceDelete) {
        CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)
      } else {
        0
      }
      // step 3: clean expired segments(MARKED_FOR_DELETE, Compacted, In Progress)
      // Since calculating the the size before and after clean files can be a costly operation
      // have exposed an option where user can change this behaviour.
      if (showStatistics) {
        val metadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
        val sizeBeforeCleaning = getPreOpSizeSnapshot(carbonTable, metadataDetails)
        checkAndCleanExpiredSegments(carbonTable, isForceDelete,
          cleanStaleInProgress, partitionSpecs)
        val sizeAfterCleaning = getPostOpSizeSnapshot(carbonTable, metadataDetails
            .map(a => a.getLoadName).toSet)
        (sizeBeforeCleaning - sizeAfterCleaning + trashFolderSizeStats._1 + deltaFileSize).abs
      } else {
        checkAndCleanExpiredSegments(carbonTable, isForceDelete,
          cleanStaleInProgress, partitionSpecs)
        0
      }
    } finally {
      if (carbonCleanFilesLock != null) {
        CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
      }
      if (carbonDeleteSegmentLock != null) {
        CarbonLockUtil.fileUnlock(carbonDeleteSegmentLock, LockUsage.DELETE_SEGMENT_LOCK)
      }
    }
  }

  /**
   * Checks the size of the segment files as well as datafiles and index files, this method
   * is used before clean files operation.
   */
  def getPreOpSizeSnapshot(carbonTable: CarbonTable, metadataDetails:
      Array[LoadMetadataDetails]): Long = {
    var size: Long = 0
    val segmentFileLocation = CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath)
    if (FileFactory.isFileExist(segmentFileLocation)) {
      size += FileFactory.getDirectorySize(segmentFileLocation)
    }
    metadataDetails.foreach(oneLoad =>
      if (oneLoad.getVisibility.toBoolean) {
        size += calculateSegmentSizeForOneLoad(carbonTable, oneLoad, metadataDetails)
      }
    )
    size
  }

  /**
   * Checks the size of the segment files as well as datafiles, this method is used after
   * clean files operation.
   */
  def getPostOpSizeSnapshot(carbonTable: CarbonTable, metadataDetails: Set[String]): Long = {
    val finalMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    var size: Long = 0
    val segmentFileLocation = CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath)
    if (FileFactory.isFileExist(segmentFileLocation)) {
      size += FileFactory.getDirectorySize(segmentFileLocation)
    }
    finalMetadataDetails.foreach(oneLoad =>
      if (metadataDetails.contains(oneLoad.getLoadName) && oneLoad.getVisibility.toBoolean) {
        size += calculateSegmentSizeForOneLoad(carbonTable, oneLoad, finalMetadataDetails)
      }
    )
    size
  }


  /**
   * Method to handle the Clean files dry run operation
   */
  def cleanFilesDryRunOperation (
      carbonTable: CarbonTable,
      isForceDelete: Boolean,
      cleanStaleInProgress: Boolean,
      showStats: Boolean): (Long, Long) = {
    // get size freed from the trash folder
    val trashFolderSizeStats = checkAndCleanTrashFolder(carbonTable, isForceDelete,
        isDryRun = true, showStats)
    // get the size of stale delete delta files that will be deleted in case any
    val deleteDeltaFileSize = if (isForceDelete) {
      CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, true)
    } else {
      0
    }
    // get size that will be deleted (MFD, COmpacted, Inprogress segments)
    val expiredSegmentsSizeStats = dryRunOnExpiredSegments(carbonTable, isForceDelete,
      cleanStaleInProgress)
    (trashFolderSizeStats._1 + expiredSegmentsSizeStats._1 + deleteDeltaFileSize,
      trashFolderSizeStats._2 + expiredSegmentsSizeStats._2)
  }

  private def checkAndCleanTrashFolder(carbonTable: CarbonTable, isForceDelete: Boolean,
      isDryRun: Boolean, showStats: Boolean): (Long, Long) = {
    if (isForceDelete) {
      // empty the trash folder
      val sizeStatistics = TrashUtil.emptyTrash(carbonTable.getTablePath, isDryRun, showStats)
      (sizeStatistics.head, sizeStatistics(1))
    } else {
      // clear trash based on timestamp
      val sizeStatistics = TrashUtil.deleteExpiredDataFromTrash(carbonTable.getTablePath,
          isDryRun, showStats)
      (sizeStatistics.head, sizeStatistics(1))
    }
  }

  /**
   * move stale segment to trash folder, but not include stale compaction (x.y) segment
   */
  private def moveStaleSegmentsToTrash(carbonTable: CarbonTable): Unit = {
    if (carbonTable.isHivePartitionTable) {
      CleanFilesUtil.cleanStaleSegmentsForPartitionTable(carbonTable)
    } else {
      CleanFilesUtil.cleanStaleSegments(carbonTable)
    }
  }

  private def checkAndCleanExpiredSegments(
      carbonTable: CarbonTable,
      isForceDelete: Boolean,
      cleanStaleInProgress: Boolean,
      partitionSpecsOption: Option[Seq[PartitionSpec]]): Unit = {
    val partitionSpecs = partitionSpecsOption.map(_.asJava).orNull
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable,
      isForceDelete, partitionSpecs, cleanStaleInProgress, true)
    if (carbonTable.isHivePartitionTable && partitionSpecsOption.isDefined) {
      SegmentFileStore.cleanSegments(carbonTable, partitionSpecs, isForceDelete)
    }
  }

  /**
   * Does Clean files dry run operation on the expired segments. Returns the size freed
   * during that clean files operation and also shows the remaining trash size, which can be
   * cleaned after those segments are expired
   */
  private def dryRunOnExpiredSegments(
      carbonTable: CarbonTable,
      isForceDelete: Boolean,
      cleanStaleInProgress: Boolean): (Long, Long) = {
    var sizeFreed: Long = 0
    var trashSizeRemaining: Long = 0
    val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    if (SegmentStatusManager.isLoadDeletionRequired(loadMetadataDetails)) {
      loadMetadataDetails.foreach { oneLoad =>
        if (!oneLoad.getVisibility.equalsIgnoreCase("false")) {
          val segmentFilePath = CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath,
              oneLoad.getSegmentFile)
          if (DeleteLoadFolders.canDeleteThisLoad(oneLoad, isForceDelete, cleanStaleInProgress,
              carbonTable.getAbsoluteTableIdentifier)) {
            // No need to consider physical data for external segments, only consider metadata.
            if (oneLoad.getPath() == null || oneLoad.getPath().equalsIgnoreCase("NA")) {
              sizeFreed += calculateSegmentSizeForOneLoad(carbonTable, oneLoad, loadMetadataDetails)
            }
            if (FileFactory.isFileExist(segmentFilePath)) {
              sizeFreed += FileFactory.getCarbonFile(segmentFilePath).getSize
            }
          } else {
            if (SegmentStatusManager.isExpiredSegment(oneLoad, carbonTable
                .getAbsoluteTableIdentifier)) {
              trashSizeRemaining += calculateSegmentSizeForOneLoad(carbonTable, oneLoad,
                  loadMetadataDetails)
              if (FileFactory.isFileExist(segmentFilePath)) {
                trashSizeRemaining += FileFactory.getCarbonFile(segmentFilePath).getSize
              }
            }
          }
        }
      }
    }
    (sizeFreed, trashSizeRemaining)
  }

  /**
   * calculates the segment size based of a segment
   */
  def calculateSegmentSizeForOneLoad( carbonTable: CarbonTable, oneLoad: LoadMetadataDetails,
        loadMetadataDetails: Array[LoadMetadataDetails]) : Long = {
    var size : Long = 0
    if (!StringUtils.isEmpty(oneLoad.getDataSize)) {
      size += oneLoad.getDataSize.toLong
    }
    if (!StringUtils.isEmpty(oneLoad.getIndexSize)) {
      size += oneLoad.getIndexSize.toLong
    }
    if (!oneLoad.getUpdateDeltaStartTimestamp.isEmpty && !oneLoad.getUpdateDeltaEndTimestamp
        .isEmpty) {
      size += calculateDeltaFileSize(carbonTable, oneLoad, loadMetadataDetails)
    }
    size
  }

  /**
   * calculates the size of delta files  for one segment
   */
  def calculateDeltaFileSize( carbonTable: CarbonTable, oneLoad: LoadMetadataDetails,
      loadMetadataDetails: Array[LoadMetadataDetails]) : Long = {
    var size: Long = 0
    val segmentUpdateStatusManager = new SegmentUpdateStatusManager(carbonTable,
        loadMetadataDetails)
    segmentUpdateStatusManager.getBlockNameFromSegment(oneLoad.getLoadName).asScala.foreach {
      block =>
      segmentUpdateStatusManager.getDeleteDeltaFilesList(Segment
          .toSegment(oneLoad.getLoadName), block).asScala.foreach{ deltaFile =>
        size += FileFactory.getCarbonFile(deltaFile).getSize
      }
    }
    size
  }

  /**
   * clean the stale compact segment immediately after compaction failure
   */
  def cleanStaleCompactionSegment(
      carbonTable: CarbonTable,
      mergedSegmentId: String,
      factTimestamp: Long,
      partitionSpecs: Option[Seq[PartitionSpec]]): Unit = {
    val metadataFolderPath = CarbonTablePath.getMetadataPath(carbonTable.getTablePath)
    val details = SegmentStatusManager.readLoadMetadata(metadataFolderPath)
    if (details == null || details.isEmpty) {
      return
    }
    val loadDetail = details.find(detail => mergedSegmentId.equals(detail.getLoadName))
    // only clean stale compaction segment
    if (loadDetail.isEmpty) {
      if (carbonTable.isHivePartitionTable && partitionSpecs.isDefined) {
        partitionSpecs.get.foreach { partitionSpec =>
          cleanStaleCompactionDataFiles(
            partitionSpec.getLocation.toString, mergedSegmentId, factTimestamp)
        }
      } else {
        cleanStaleCompactionDataFiles(
          CarbonTablePath.getSegmentPath(carbonTable.getTablePath, mergedSegmentId),
          mergedSegmentId,
          factTimestamp)
      }
    }
  }

  private def cleanStaleCompactionDataFiles(
      folderPath: String,
      segmentId: String,
      factTimestamp: Long): Unit = {
    if (FileFactory.isFileExist(folderPath)) {
      val namePart = CarbonCommonConstants.HYPHEN + segmentId +
        CarbonCommonConstants.HYPHEN + factTimestamp
      val toBeDeleted = FileFactory.getCarbonFile(folderPath).listFiles(new CarbonFileFilter() {
        override def accept(file: CarbonFile): Boolean = {
          file.getName.contains(namePart)
        }
      })
      if (toBeDeleted != null && toBeDeleted.nonEmpty) {
        try {
          CarbonUtil.deleteFoldersAndFilesSilent(toBeDeleted: _*)
        } catch {
          case e: Throwable =>
            LOGGER.error(
              s"Failed to clean stale data under folder $folderPath, match filter: $namePart", e)
        }
      }
    }
  }
}
