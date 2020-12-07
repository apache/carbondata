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

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, CleanFilesUtil, TrashUtil}
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
      partitionSpecs: Option[Seq[PartitionSpec]] = None): Unit = {
    // if isForceDelete = true need to throw exception if CARBON_CLEAN_FILES_FORCE_ALLOWED is false
    if (isForceDelete && !CarbonProperties.getInstance().isCleanFilesForceAllowed) {
      LOGGER.error("Clean Files with Force option deletes the physical data and it cannot be" +
        " recovered. It is disabled by default, to enable clean files with force option," +
        " set " + CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED + " to true")
      throw new RuntimeException("Clean files with force operation not permitted by default")
    }
    var carbonCleanFilesLock: ICarbonLock = null
    try {
      val errorMsg = "Clean files request is failed for " +
        s"${ carbonTable.getQualifiedName }" +
        ". Not able to acquire the clean files lock due to another clean files " +
        "operation is running in the background."
      carbonCleanFilesLock = CarbonLockUtil.getLockObject(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.CLEAN_FILES_LOCK, errorMsg)
      // step 1: check and clean trash folder
      checkAndCleanTrashFolder(carbonTable, isForceDelete)
      // step 2: move stale segments which are not exists in metadata into .Trash
      moveStaleSegmentsToTrash(carbonTable)
      // step 3: clean expired segments(MARKED_FOR_DELETE, Compacted, In Progress)
      checkAndCleanExpiredSegments(carbonTable, isForceDelete, cleanStaleInProgress, partitionSpecs)
    } finally {
      if (carbonCleanFilesLock != null) {
        CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
      }
    }
  }

  private def checkAndCleanTrashFolder(carbonTable: CarbonTable, isForceDelete: Boolean): Unit = {
    if (isForceDelete) {
      // empty the trash folder
      TrashUtil.emptyTrash(carbonTable.getTablePath)
    } else {
      // clear trash based on timestamp
      TrashUtil.deleteExpiredDataFromTrash(carbonTable.getTablePath)
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
