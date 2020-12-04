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
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CleanFilesUtil, TrashUtil}

/**
 * This object will manage the following data.
 * 1. .Trash folder
 * 2. stale segments without metadata
 * 3. expired segments (MARKED_FOR_DELETE, Compacted, In Progress)
 */
object DataTrashManager {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * clean garbage data
   *  1. check and clean .Trash folder
   *  2. move stale segments without metadata into .Trash
   *  3. clean expired segments(MARKED_FOR_DELETE, Compacted, In Progress)
   */
  def cleanGarbageData(
      carbonTable: CarbonTable,
      force: Boolean = false,
      partitionSpecs: Option[Seq[PartitionSpec]] = None): Unit = {
    var carbonCleanFilesLock: ICarbonLock = null
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    try {
      val errorMsg = "Clean files request is failed for " +
        s"${ carbonTable.getQualifiedName }" +
        ". Not able to acquire the clean files lock due to another clean files " +
        "operation is running in the background."
      carbonCleanFilesLock = CarbonLockUtil.getLockObject(absoluteTableIdentifier,
        LockUsage.CLEAN_FILES_LOCK, errorMsg)
      // step 1: check and clean trash folder
      checkAndCleanTrashFolder(carbonTable, force)
      // step 2: move stale segments which are not exists in metadata into .Trash
      moveStaleSegmentsToTrash(carbonTable)
      // step 3: clean expired segments(MARKED_FOR_DELETE, Compacted, In Progress)
      cleanExpiredSegments(carbonTable, force, partitionSpecs)
    } finally {
      if (carbonCleanFilesLock != null) {
        CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
      }
    }
  }

  private def checkAndCleanTrashFolder(carbonTable: CarbonTable, forceClean: Boolean): Unit = {
    if (forceClean) {
      // empty the trash folder
      if (CarbonProperties.getInstance().isCleanFilesForceAllowed) {
        TrashUtil.emptyTrash(carbonTable.getTablePath)
      } else {
        LOGGER.error("Clean Files with Force option deletes the physical data and it cannot be" +
          " recovered. It is disabled by default, to enable clean files with force option," +
          " set " + CarbonCommonConstants.CARBON_CLEAN_FILES_FORCE_ALLOWED + " to true")
        throw new RuntimeException("Clean files with force operation not permitted by default")
      }
    } else {
      // clear trash based on timestamp
      TrashUtil.deleteExpiredDataFromTrash(carbonTable.getTablePath)
    }
  }

  private def moveStaleSegmentsToTrash(carbonTable: CarbonTable): Unit = {
    if (carbonTable.isHivePartitionTable) {
      CleanFilesUtil.cleanStaleSegmentsForPartitionTable(carbonTable)
    } else {
      CleanFilesUtil.cleanStaleSegments(carbonTable)
    }
  }

  private def cleanExpiredSegments(
      carbonTable: CarbonTable,
      force: Boolean,
      partitionSpecsOption: Option[Seq[PartitionSpec]]): Unit = {
    val partitionSpecs = partitionSpecsOption.map(_.asJava).orNull
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, force, partitionSpecs)
    // clean expired partitions in SUCCESS segments
    if (carbonTable.isHivePartitionTable && partitionSpecsOption.isDefined) {
      SegmentFileStore.cleanSegments(carbonTable, partitionSpecs, force)
    }
  }

}
