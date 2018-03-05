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
import java.util.List
import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel, CompactionModel}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.MergeResultImpl
import org.apache.carbondata.spark.util.CommonUtil

/**
 * This class is used to perform compaction on carbon table.
 */
class CarbonTableCompactor(carbonLoadModel: CarbonLoadModel,
    compactionModel: CompactionModel,
    executor: ExecutorService,
    sqlContext: SQLContext,
    storeLocation: String,
    operationContext: OperationContext)
  extends Compactor(carbonLoadModel, compactionModel, executor, sqlContext, storeLocation) {

  override def executeCompaction(): Unit = {
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
           (CompactionType.IUD_UPDDEL_DELTA == compactionModel.compactionType &&
            loadsToMerge.size() > 0)) {
      val lastSegment = sortedSegments.get(sortedSegments.size() - 1)
      deletePartialLoadsInCompaction()

      try {
        scanSegmentsAndSubmitJob(loadsToMerge)
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
      if (CompactionType.MAJOR == compactionModel.compactionType) {

        segList = CarbonDataMergerUtil
          .filterOutNewlyAddedSegments(carbonLoadModel.getLoadMetadataDetails, lastSegment)
      }

      if (CompactionType.IUD_UPDDEL_DELTA == compactionModel.compactionType) {
        loadsToMerge.clear()
      } else if (segList.size > 0) {
        loadsToMerge = CarbonDataMergerUtil.identifySegmentsToBeMerged(
          carbonLoadModel,
          compactionModel.compactionSize,
          segList,
          compactionModel.compactionType
        )
        if (carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.isHivePartitionTable) {
          carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
        }
      }
      else {
        loadsToMerge.clear()
      }
    }
  }

  /**
   * This will submit the loads to be merged into the executor.
   */
  def scanSegmentsAndSubmitJob(loadsToMerge: util.List[LoadMetadataDetails]): Unit = {
    loadsToMerge.asScala.foreach { seg =>
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }
    val compactionCallableModel = CompactionCallableModel(
      carbonLoadModel,
      compactionModel.carbonTable,
      loadsToMerge,
      sqlContext,
      compactionModel.compactionType,
      compactionModel.currentPartitions)
    triggerCompaction(compactionCallableModel)
  }

  private def triggerCompaction(compactionCallableModel: CompactionCallableModel): Unit = {
    val carbonTable = compactionCallableModel.carbonTable
    val loadsToMerge = compactionCallableModel.loadsToMerge
    val sc = compactionCallableModel.sqlContext
    val carbonLoadModel = compactionCallableModel.carbonLoadModel
    val compactionType = compactionCallableModel.compactionType
    val partitions = compactionCallableModel.currentPartitions
    val tablePath = carbonLoadModel.getTablePath
    val startTime = System.nanoTime()
    val mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
    var finalMergeStatus = false
    val databaseName: String = carbonLoadModel.getDatabaseName
    val factTableName = carbonLoadModel.getTableName
    val validSegments: List[Segment] = CarbonDataMergerUtil.getValidSegments(loadsToMerge)
    val carbonMergerMapping = CarbonMergerMapping(
      tablePath,
      carbonTable.getMetaDataFilepath,
      mergedLoadName,
      databaseName,
      factTableName,
      validSegments.asScala.toArray,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
      compactionType,
      maxSegmentColCardinality = null,
      maxSegmentColumnSchemaList = null,
      currentPartitions = partitions)
    carbonLoadModel.setTablePath(carbonMergerMapping.hdfsStoreLocation)
    carbonLoadModel.setLoadMetadataDetails(
      SegmentStatusManager.readLoadMetadata(carbonTable.getMetaDataFilepath).toList.asJava)
    // trigger event for compaction
    val alterTableCompactionPreEvent: AlterTableCompactionPreEvent =
      AlterTableCompactionPreEvent(sqlContext.sparkSession,
        carbonTable,
        carbonMergerMapping,
        mergedLoadName)
    OperationListenerBus.getInstance.fireEvent(alterTableCompactionPreEvent, operationContext)

    var execInstance = "1"
    // in case of non dynamic executor allocation, number of executors are fixed.
    if (sc.sparkContext.getConf.contains("spark.executor.instances")) {
      execInstance = sc.sparkContext.getConf.get("spark.executor.instances")
      LOGGER.info(s"spark.executor.instances property is set to = $execInstance")
    } // in case of dynamic executor allocation, taking the max executors of the dynamic allocation.
    else if (sc.sparkContext.getConf.contains("spark.dynamicAllocation.enabled")) {
      if (sc.sparkContext.getConf.get("spark.dynamicAllocation.enabled").trim
        .equalsIgnoreCase("true")) {
        execInstance = sc.sparkContext.getConf.get("spark.dynamicAllocation.maxExecutors")
        LOGGER.info(s"spark.dynamicAllocation.maxExecutors property is set to = $execInstance")
      }
    }

    val mergeStatus =
      if (CompactionType.IUD_UPDDEL_DELTA == compactionType) {
        new CarbonIUDMergerRDD(
          sc.sparkContext,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping,
          execInstance
        ).collect
      } else {
        new CarbonMergerRDD(
          sc.sparkContext,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping,
          execInstance
        ).collect
      }

    if (mergeStatus.length == 0) {
      finalMergeStatus = false
    } else {
      finalMergeStatus = mergeStatus.forall(_._2)
    }

    if (finalMergeStatus) {
      val mergedLoadNumber = CarbonDataMergerUtil.getLoadNumberFromLoadName(mergedLoadName)
      var segmentFileName: String = null
      if (carbonTable.isHivePartitionTable) {
        val readPath =
          CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath) +
          CarbonCommonConstants.FILE_SEPARATOR + carbonLoadModel.getFactTimeStamp + ".tmp"
        // Merge all partition files into a single file.
        segmentFileName =
          mergedLoadNumber + "_" + carbonLoadModel.getFactTimeStamp
        val segmentFile = SegmentFileStore
          .mergeSegmentFiles(readPath,
            segmentFileName,
            CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath))
        if (segmentFile != null) {
          SegmentFileStore
            .moveFromTempFolder(segmentFile,
              carbonLoadModel.getFactTimeStamp + ".tmp",
              carbonLoadModel.getTablePath)
        }
        segmentFileName = segmentFileName + CarbonTablePath.SEGMENT_EXT
      }
      // trigger event for compaction
      val alterTableCompactionPreStatusUpdateEvent =
      AlterTableCompactionPreStatusUpdateEvent(sc.sparkSession,
        carbonTable,
        carbonMergerMapping,
        carbonLoadModel,
        mergedLoadName)
      OperationListenerBus.getInstance
        .fireEvent(alterTableCompactionPreStatusUpdateEvent, operationContext)

      val endTime = System.nanoTime()
      LOGGER.info(s"time taken to merge $mergedLoadName is ${ endTime - startTime }")
      val statusFileUpdation =
        ((compactionType == CompactionType.IUD_UPDDEL_DELTA) &&
         CarbonDataMergerUtil
           .updateLoadMetadataIUDUpdateDeltaMergeStatus(loadsToMerge,
             carbonTable.getMetaDataFilepath,
             carbonLoadModel)) ||
        CarbonDataMergerUtil.updateLoadMetadataWithMergeStatus(
          loadsToMerge,
            carbonTable.getMetaDataFilepath,
            mergedLoadNumber,
          carbonLoadModel,
          compactionType,
          segmentFileName)

      val compactionLoadStatusPostEvent = AlterTableCompactionPostStatusUpdateEvent(sc.sparkSession,
        carbonTable,
        carbonMergerMapping,
        carbonLoadModel,
        mergedLoadName)
      // Used to inform the commit listener that the commit is fired from compaction flow.
      operationContext.setProperty("isCompaction", "true")
      val commitComplete = try {
        // Once main table compaction is done and 0.1, 4.1, 8.1 is created commit will happen for
        // all the tables. The commit listener will compact the child tables until no more segments
        // are left. But 2nd level compaction is yet to happen on the main table therefore again the
        // compaction flow will try to commit the child tables which is wrong. This check tell the
        // 2nd level compaction flow that the commit for datamaps is already done.
        val isCommitDone = operationContext.getProperty("commitComplete")
        if (isCommitDone != null) {
          isCommitDone.toString.toBoolean
        } else {
          OperationListenerBus.getInstance()
            .fireEvent(compactionLoadStatusPostEvent, operationContext)
          true
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex, "Problem while committing data maps")
          false
      }
      operationContext.setProperty("commitComplete", commitComplete)
      // here either of the conditions can be true, when delete segment is fired after compaction
      // has started, statusFileUpdation will be false , but at the same time commitComplete can be
      // true because compaction for all datamaps will be finished at a time to the maximum level
      // possible (level 1, 2 etc). so we need to check for either condition
      if (!statusFileUpdation || !commitComplete) {
        LOGGER.audit(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        LOGGER.error(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        throw new Exception(s"Compaction failed to update metadata for table" +
                            s" ${ carbonLoadModel.getDatabaseName }." +
                            s"${ carbonLoadModel.getTableName }")
      } else {
        LOGGER.audit(s"Compaction request completed for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.info(s"Compaction request completed for table " +
                    s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      }
    } else {
      LOGGER.audit(s"Compaction request failed for table " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }"
      )
      LOGGER.error(s"Compaction request failed for table " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      throw new Exception("Compaction Failure in Merger Rdd.")
    }
  }

}
