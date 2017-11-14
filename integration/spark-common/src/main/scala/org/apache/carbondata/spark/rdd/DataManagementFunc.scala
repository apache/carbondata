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

package org.apache.carbondata.spark.rdd

import java.util
import java.util.concurrent._

import scala.collection.JavaConverters._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.{CompactionCallableModel, CompactionModel}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.{CarbonLoaderUtil, DeleteLoadFolders}
import org.apache.carbondata.spark.compaction.CompactionCallable
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Common functions for data life cycle management
 */
object DataManagementFunc {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def executeCompaction(carbonLoadModel: CarbonLoadModel,
      compactionModel: CompactionModel,
      executor: ExecutorService,
      sqlContext: SQLContext,
      storeLocation: String): Unit = {
    val sortedSegments: util.List[LoadMetadataDetails] = new util.ArrayList[LoadMetadataDetails](
      carbonLoadModel.getLoadMetadataDetails
    )
    CarbonDataMergerUtil.sortSegments(sortedSegments)

    var segList = carbonLoadModel.getLoadMetadataDetails
    var loadsToMerge = CarbonDataMergerUtil.identifySegmentsToBeMerged(
      carbonLoadModel,
      compactionModel.compactionSize,
      segList,
      compactionModel.compactionType
    )
    while (loadsToMerge.size() > 1 ||
           (compactionModel.compactionType.name().equals("IUD_UPDDEL_DELTA_COMPACTION") &&
            loadsToMerge.size() > 0)) {
      val lastSegment = sortedSegments.get(sortedSegments.size() - 1)
      deletePartialLoadsInCompaction(carbonLoadModel)
      val futureList: util.List[Future[Void]] = new util.ArrayList[Future[Void]](
        CarbonCommonConstants
            .DEFAULT_COLLECTION_SIZE
      )

      scanSegmentsAndSubmitJob(futureList,
        loadsToMerge,
        executor,
        sqlContext,
        compactionModel,
        carbonLoadModel,
        storeLocation
      )

      try {

        futureList.asScala.foreach(future => {
          future.get
        }
        )
      } catch {
        case e: Exception =>
          LOGGER.error(e, s"Exception in compaction thread ${ e.getMessage }")
          throw e
      }

      // scan again and determine if anything is there to merge again.
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
      segList = carbonLoadModel.getLoadMetadataDetails
      // in case of major compaction we will scan only once and come out as it will keep
      // on doing major for the new loads also.
      // excluding the newly added segments.
      if (compactionModel.compactionType == CompactionType.MAJOR_COMPACTION) {

        segList = CarbonDataMergerUtil
          .filterOutNewlyAddedSegments(carbonLoadModel.getLoadMetadataDetails, lastSegment)
      }

      if (compactionModel.compactionType == CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
        loadsToMerge.clear()
      } else if (segList.size > 0) {
        loadsToMerge = CarbonDataMergerUtil.identifySegmentsToBeMerged(
          carbonLoadModel,
          compactionModel.compactionSize,
          segList,
          compactionModel.compactionType
        )
      }
      else {
        loadsToMerge.clear()
      }
    }
  }

  /**
   * This will submit the loads to be merged into the executor.
   */
  private def scanSegmentsAndSubmitJob(futureList: util.List[Future[Void]],
      loadsToMerge: util.List[LoadMetadataDetails],
      executor: ExecutorService,
      sqlContext: SQLContext,
      compactionModel: CompactionModel,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String
  ): Unit = {
    loadsToMerge.asScala.foreach { seg =>
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }

    val compactionCallableModel = CompactionCallableModel(
      carbonLoadModel,
      storeLocation,
      compactionModel.carbonTable,
      loadsToMerge,
      sqlContext,
      compactionModel.compactionType
    )

    val future: Future[Void] = executor.submit(new CompactionCallable(compactionCallableModel))
    futureList.add(future)
  }

  def deletePartialLoadsInCompaction(carbonLoadModel: CarbonLoadModel): Unit = {
    // Deleting the any partially loaded data if present.
    // in some case the segment folder which is present in store will not have entry in
    // status.
    // so deleting those folders.
    try {
      CarbonLoaderUtil.deletePartialLoadDataIfExist(carbonLoadModel, true)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in compaction thread while clean up of stale segments" +
            s" ${ e.getMessage }")
    }
  }

  private def isLoadDeletionRequired(metaDataLocation: String): Boolean = {
    val details = SegmentStatusManager.readLoadMetadata(metaDataLocation)
    if (details != null && details.nonEmpty) for (oneRow <- details) {
      if ((SegmentStatus.MARKED_FOR_DELETE == oneRow.getSegmentStatus ||
           SegmentStatus.COMPACTED == oneRow.getSegmentStatus) &&
          oneRow.getVisibility.equalsIgnoreCase("true")) {
        return true
      }
    }
    false
  }

  def deleteLoadsAndUpdateMetadata(
      dbName: String,
      tableName: String,
      storePath: String,
      isForceDeletion: Boolean,
      carbonTable: CarbonTable): Unit = {
    if (isLoadDeletionRequired(carbonTable.getMetaDataFilepath)) {
      val details = SegmentStatusManager.readLoadMetadata(carbonTable.getMetaDataFilepath)
      val carbonTableStatusLock =
        CarbonLockFactory.getCarbonLockObj(
          new CarbonTableIdentifier(dbName, tableName, ""),
          LockUsage.TABLE_STATUS_LOCK
        )

      // Delete marked loads
      val isUpdationRequired =
        DeleteLoadFolders.deleteLoadFoldersFromFileSystem(
          dbName,
          tableName,
          storePath,
          isForceDeletion,
          details
        )

      if (isUpdationRequired) {
        try {
          // Update load metadate file after cleaning deleted nodes
          if (carbonTableStatusLock.lockWithRetries()) {
            LOGGER.info("Table status lock has been successfully acquired.")

            // read latest table status again.
            val latestMetadata = SegmentStatusManager
              .readLoadMetadata(carbonTable.getMetaDataFilepath)

            // update the metadata details from old to new status.
            val latestStatus = CarbonLoaderUtil
                .updateLoadMetadataFromOldToNew(details, latestMetadata)

            CarbonLoaderUtil.writeLoadMetadata(storePath, dbName, tableName, latestStatus)
          } else {
            val errorMsg = "Clean files request is failed for " +
                s"$dbName.$tableName" +
                ". Not able to acquire the table status lock due to other operation " +
                "running in the background."
            LOGGER.audit(errorMsg)
            LOGGER.error(errorMsg)
            throw new Exception(errorMsg + " Please try after some time.")
          }
        } finally {
          CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK)
        }
      }
    }
  }

}
