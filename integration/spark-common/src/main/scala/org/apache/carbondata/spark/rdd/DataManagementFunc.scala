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
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.{CompactionCallableModel, CompactionModel, DropPartitionCallableModel, SplitPartitionCallableModel}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.{CarbonLoaderUtil, DeleteLoadFolders, LoadMetadataUtil}
import org.apache.carbondata.spark._
import org.apache.carbondata.spark.compaction.CompactionCallable
import org.apache.carbondata.spark.partition.{DropPartitionCallable, SplitPartitionCallable}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Common functions for data life cycle management
 */
object DataManagementFunc {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def deleteLoadByDate(
      sqlContext: SQLContext,
      schema: CarbonDataLoadSchema,
      databaseName: String,
      tableName: String,
      storePath: String,
      dateField: String,
      dateFieldActualName: String,
      dateValue: String) {

    val sc = sqlContext
    // Delete the records based on data
    val table = schema.getCarbonTable
    val loadMetadataDetailsArray =
      SegmentStatusManager.readLoadMetadata(table.getMetaDataFilepath).toList
    val resultMap = new CarbonDeleteLoadByDateRDD(
      sc.sparkContext,
      new DeletedLoadResultImpl(),
      databaseName,
      table.getDatabaseName,
      dateField,
      dateFieldActualName,
      dateValue,
      table.getFactTableName,
      tableName,
      storePath,
      loadMetadataDetailsArray).collect.groupBy(_._1)

    var updatedLoadMetadataDetailsList = new ListBuffer[LoadMetadataDetails]()
    if (resultMap.nonEmpty) {
      if (resultMap.size == 1) {
        if (resultMap.contains("")) {
          LOGGER.error("Delete by Date request is failed")
          sys.error("Delete by Date request is failed, potential causes " +
              "Empty store or Invalid column type, For more details please refer logs.")
        }
      }
      val updatedloadMetadataDetails = loadMetadataDetailsArray.map { elem => {
        var statusList = resultMap.get(elem.getLoadName)
        // check for the merged load folder.
        if (statusList.isEmpty && null != elem.getMergedLoadName) {
          statusList = resultMap.get(elem.getMergedLoadName)
        }

        if (statusList.isDefined) {
          elem.setModificationOrdeletionTimesStamp(elem.getTimeStamp(CarbonLoaderUtil
            .readCurrentTime()))
          // if atleast on CarbonCommonConstants.MARKED_FOR_UPDATE status exist,
          // use MARKED_FOR_UPDATE
          if (statusList.get
              .forall(status => status._2 == CarbonCommonConstants.MARKED_FOR_DELETE)) {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE)
          } else {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE)
            updatedLoadMetadataDetailsList += elem
          }
          elem
        } else {
          elem
        }
      }

      }

      // Save the load metadata
      val carbonLock = CarbonLockFactory
          .getCarbonLockObj(table.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
            LockUsage.METADATA_LOCK
          )
      try {
        if (carbonLock.lockWithRetries()) {
          LOGGER.info("Successfully got the table metadata file lock")
          if (updatedLoadMetadataDetailsList.nonEmpty) {
            // TODO: Load Aggregate tables after retention.
          }

          // write
          CarbonLoaderUtil.writeLoadMetadata(
            storePath,
            databaseName,
            table.getDatabaseName,
            updatedloadMetadataDetails.asJava
          )
        }
      } finally {
        if (carbonLock.unlock()) {
          LOGGER.info("unlock the table metadata file successfully")
        } else {
          LOGGER.error("Unable to unlock the metadata lock")
        }
      }
    } else {
      LOGGER.error("Delete by Date request is failed")
      LOGGER.audit(s"The delete load by date is failed for $databaseName.$tableName")
      sys.error("Delete by Date request is failed, potential causes " +
          "Empty store or Invalid column type, For more details please refer logs.")
    }
  }

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
   *
   * @param futureList
   */
  private def scanSegmentsAndSubmitJob(futureList: util.List[Future[Void]],
      loadsToMerge: util.List[LoadMetadataDetails],
      executor: ExecutorService,
      sqlContext: SQLContext,
      compactionModel: CompactionModel,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String): Unit = {

    loadsToMerge.asScala.foreach(seg => {
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }
    )

    val compactionCallableModel = CompactionCallableModel(carbonLoadModel,
      storeLocation,
      compactionModel.carbonTable,
      loadsToMerge,
      sqlContext,
      compactionModel.compactionType
    )

    val future: Future[Void] = executor.submit(new CompactionCallable(compactionCallableModel))
    futureList.add(future)
  }

  def executePartitionSplit( sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      executor: ExecutorService,
      segment: String,
      partitionId: String,
      oldPartitionIdList: List[Int]): Unit = {
    val futureList: util.List[Future[Void]] = new util.ArrayList[Future[Void]](
      CarbonCommonConstants.DEFAULT_COLLECTION_SIZE
    )
    scanSegmentsForSplitPartition(futureList, executor, segment, partitionId,
      sqlContext, carbonLoadModel, oldPartitionIdList)
    try {
        futureList.asScala.foreach(future => {
          future.get
        }
      )
    } catch {
      case e: Exception =>
        LOGGER.error(e, s"Exception in partition split thread ${ e.getMessage }")
        throw e
    }
  }

  private def scanSegmentsForSplitPartition(futureList: util.List[Future[Void]],
      executor: ExecutorService,
      segmentId: String,
      partitionId: String,
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      oldPartitionIdList: List[Int]): Unit = {

    val splitModel = SplitPartitionCallableModel(carbonLoadModel,
      segmentId,
      partitionId,
      oldPartitionIdList,
      sqlContext)

    val future: Future[Void] = executor.submit(new SplitPartitionCallable(splitModel))
    futureList.add(future)
  }

  def executeDroppingPartition(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      executor: ExecutorService,
      segmentId: String,
      partitionId: String,
      dropWithData: Boolean,
      oldPartitionIds: List[Int]): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val model = new DropPartitionCallableModel(carbonLoadModel,
      segmentId, partitionId, oldPartitionIds, dropWithData, carbonTable, sqlContext)
    val future: Future[Void] = executor.submit(new DropPartitionCallable(model))
    try {
        future.get
    } catch {
      case e: Exception =>
        LOGGER.error(e, s"Exception in partition drop thread ${ e.getMessage }")
        throw e
    }
  }

  def prepareCarbonLoadModel(table: CarbonTable, newCarbonLoadModel: CarbonLoadModel): Unit = {
    newCarbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    newCarbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    newCarbonLoadModel.setTableName(table.getCarbonTableIdentifier.getTableName)
    newCarbonLoadModel.setDatabaseName(table.getCarbonTableIdentifier.getDatabaseName)
    newCarbonLoadModel.setStorePath(table.getStorePath)
    CommonUtil.readLoadMetadataDetails(newCarbonLoadModel)
    val loadStartTime = CarbonUpdateUtil.readCurrentTime();
    newCarbonLoadModel.setFactTimeStamp(loadStartTime)
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

  def deleteLoadsAndUpdateMetadata(
      dbName: String,
      tableName: String,
      storePath: String,
      isForceDeletion: Boolean,
      carbonTable: CarbonTable): Unit = {
    if (LoadMetadataUtil.isLoadDeletionRequired(carbonTable.getMetaDataFilepath)) {
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

  def cleanFiles(
      dbName: String,
      tableName: String,
      storePath: String,
      carbonTable: CarbonTable,
      forceTableClean: Boolean): Unit = {
    val identifier = new CarbonTableIdentifier(dbName, tableName, "")
    val carbonCleanFilesLock =
      CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.CLEAN_FILES_LOCK)
    try {
      if (carbonCleanFilesLock.lockWithRetries()) {
        LOGGER.info("Clean files lock has been successfully acquired.")
        if (forceTableClean) {
          val absIdent = AbsoluteTableIdentifier.from(storePath, dbName, tableName)
          FileFactory.deleteAllCarbonFilesOfDir(
            FileFactory.getCarbonFile(absIdent.getTablePath,
            FileFactory.getFileType(absIdent.getTablePath)))
        } else {
          deleteLoadsAndUpdateMetadata(dbName, tableName, storePath,
            isForceDeletion = true, carbonTable)
          CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, true)
        }
      } else {
        val errorMsg = "Clean files request is failed for " +
            s"$dbName.$tableName" +
            ". Not able to acquire the clean files lock due to another clean files " +
            "operation is running in the background."
        LOGGER.audit(errorMsg)
        LOGGER.error(errorMsg)
        throw new Exception(errorMsg + " Please try after some time.")
      }
    } finally {
      CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
    }
  }
}
