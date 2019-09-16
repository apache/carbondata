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
import scala.collection.mutable

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel, CompactionModel}
import org.apache.spark.util.MergeIndexUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CarbonCompactionUtil, CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.{MergeResultImpl, SegmentUtils}
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark

/**
 * This class is used to perform compaction on carbon table.
 */
class CarbonTableCompactor(carbonLoadModel: CarbonLoadModel,
    compactionModel: CompactionModel,
    executor: ExecutorService,
    sqlContext: SQLContext,
    storeLocation: String,
    compactedSegments: List[String],
    operationContext: OperationContext)
  extends Compactor(carbonLoadModel, compactionModel, executor, sqlContext, storeLocation) {

  private def needSortSingleSegment(
      loadsToMerge: java.util.List[LoadMetadataDetails]): Boolean = {
    // support to resort old segment with old sort_columns
    if (CompactionType.CUSTOM == compactionModel.compactionType &&
        loadsToMerge.size() == 1 &&
        SortScope.NO_SORT != compactionModel.carbonTable.getSortScope) {
      !CarbonCompactionUtil.isSortedByCurrentSortColumns(
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        loadsToMerge.get(0),
        FileFactory.getConfiguration)
    } else {
      false
    }
  }

  override def executeCompaction(): Unit = {
    val sortedSegments: util.List[LoadMetadataDetails] = new util.ArrayList[LoadMetadataDetails](
      carbonLoadModel.getLoadMetadataDetails
    )
    CarbonDataMergerUtil.sortSegments(sortedSegments)

    var loadsToMerge = identifySegmentsToBeMerged()

    while (loadsToMerge.size() > 1 || needSortSingleSegment(loadsToMerge) ||
           (CompactionType.IUD_UPDDEL_DELTA == compactionModel.compactionType &&
            loadsToMerge.size() > 0)) {
      val lastSegment = sortedSegments.get(sortedSegments.size() - 1)
      deletePartialLoadsInCompaction()

      try {
        scanSegmentsAndSubmitJob(loadsToMerge, compactedSegments)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in compaction thread ${ e.getMessage }", e)
          throw e
      }

      // scan again and determine if anything is there to merge again.
      carbonLoadModel.readAndSetLoadMetadataDetails()
      var segList = carbonLoadModel.getLoadMetadataDetails
      // in case of major compaction we will scan only once and come out as it will keep
      // on doing major for the new loads also.
      // excluding the newly added segments.
      if (CompactionType.MAJOR == compactionModel.compactionType) {

        segList = CarbonDataMergerUtil
          .filterOutNewlyAddedSegments(carbonLoadModel.getLoadMetadataDetails, lastSegment)
      }

      if (CompactionType.IUD_UPDDEL_DELTA == compactionModel.compactionType ||
        CompactionType.CUSTOM == compactionModel.compactionType) {
        loadsToMerge.clear()
      } else if (segList.size > 0) {
        loadsToMerge = identifySegmentsToBeMerged()

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
  def scanSegmentsAndSubmitJob(loadsToMerge: util.List[LoadMetadataDetails],
      compactedSegments: List[String]): Unit = {
    loadsToMerge.asScala.foreach { seg =>
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }
    val compactionCallableModel = CompactionCallableModel(
      carbonLoadModel,
      compactionModel.carbonTable,
      loadsToMerge,
      sqlContext,
      compactionModel.compactionType,
      compactionModel.currentPartitions,
      compactedSegments)
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
    val mergedLoads = compactionCallableModel.compactedSegments
    mergedLoads.add(mergedLoadName)
    var finalMergeStatus = false
    val databaseName: String = carbonLoadModel.getDatabaseName
    val factTableName = carbonLoadModel.getTableName
    val validSegments: List[Segment] = CarbonDataMergerUtil.getValidSegments(loadsToMerge)
    val carbonMergerMapping = CarbonMergerMapping(
      tablePath,
      carbonTable.getMetadataPath,
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
      SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath).toList.asJava)
    // trigger event for compaction
    val alterTableCompactionPreEvent: AlterTableCompactionPreEvent =
      AlterTableCompactionPreEvent(sqlContext.sparkSession,
        carbonTable,
        carbonMergerMapping,
        mergedLoadName)
    OperationListenerBus.getInstance.fireEvent(alterTableCompactionPreEvent, operationContext)
    // Add pre event listener for index datamap
    val tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(carbonTable)
    val dataMapOperationContext = new OperationContext()
    if (null != tableDataMaps) {
      val dataMapNames: mutable.Buffer[String] =
        tableDataMaps.asScala.map(dataMap => dataMap.getDataMapSchema.getDataMapName)
      val dataMapPreExecutionEvent: BuildDataMapPreExecutionEvent =
        new BuildDataMapPreExecutionEvent(sqlContext.sparkSession,
        carbonTable.getAbsoluteTableIdentifier, dataMapNames)
      OperationListenerBus.getInstance().fireEvent(dataMapPreExecutionEvent,
        dataMapOperationContext)
    }

    val mergeStatus =
      if (CompactionType.IUD_UPDDEL_DELTA == compactionType) {
        new CarbonIUDMergerRDD(
          sc.sparkSession,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping
        ).collect
      } else if (CompactionType.MINOR == compactionType &&
                 SortScope.GLOBAL_SORT.equals(carbonTable.getSortScope)) {
        globalSortForMinor(sc.sparkSession, carbonLoadModel, carbonMergerMapping)
      } else {
        new CarbonMergerRDD(
          sc.sparkSession,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping
        ).collect
      }

    if (mergeStatus.length == 0) {
      finalMergeStatus = false
    } else {
      finalMergeStatus = mergeStatus.forall(_._2)
    }

    if (finalMergeStatus) {
      val mergedLoadNumber = CarbonDataMergerUtil.getLoadNumberFromLoadName(mergedLoadName)
      var segmentFilesForIUDCompact = new util.ArrayList[Segment]()
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
      } else {
        // Get the segment files each updated segment in case of IUD compaction
        if (compactionType == CompactionType.IUD_UPDDEL_DELTA) {
          val segmentFilesList = loadsToMerge.asScala.map{seg =>
            val file = SegmentFileStore.writeSegmentFile(
              carbonTable,
              seg.getLoadName,
              carbonLoadModel.getFactTimeStamp.toString)
            new Segment(seg.getLoadName, file)
          }.filter(_.getSegmentFileName != null).asJava
          segmentFilesForIUDCompact = new util.ArrayList[Segment](segmentFilesList)
        } else {
          segmentFileName = SegmentFileStore.writeSegmentFile(
            carbonTable,
            mergedLoadNumber,
            carbonLoadModel.getFactTimeStamp.toString)
        }
      }
      // Used to inform the commit listener that the commit is fired from compaction flow.
      operationContext.setProperty("isCompaction", "true")
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
             carbonTable.getMetadataPath,
             carbonLoadModel,
             segmentFilesForIUDCompact)) ||
        CarbonDataMergerUtil.updateLoadMetadataWithMergeStatus(
          loadsToMerge,
          carbonTable.getMetadataPath,
          mergedLoadNumber,
          carbonLoadModel,
          compactionType,
          segmentFileName)

      if (compactionType != CompactionType.IUD_DELETE_DELTA &&
          compactionType != CompactionType.IUD_UPDDEL_DELTA) {
        MergeIndexUtil.mergeIndexFilesOnCompaction(compactionCallableModel)
      }

      val compactionLoadStatusPostEvent = AlterTableCompactionPostStatusUpdateEvent(sc.sparkSession,
        carbonTable,
        carbonMergerMapping,
        carbonLoadModel,
        mergedLoadName)
      OperationListenerBus.getInstance()
        .fireEvent(compactionLoadStatusPostEvent, operationContext)
      if (null != tableDataMaps) {
        val buildDataMapPostExecutionEvent = new BuildDataMapPostExecutionEvent(
          sqlContext.sparkSession, carbonTable.getAbsoluteTableIdentifier,
          null, Seq(mergedLoadNumber), true)
        OperationListenerBus.getInstance()
          .fireEvent(buildDataMapPostExecutionEvent, dataMapOperationContext)
      }
      val commitDone = operationContext.getProperty("commitComplete")
      val commitComplete = if (null != commitDone) {
        commitDone.toString.toBoolean
      } else {
        true
      }
      // here either of the conditions can be true, when delete segment is fired after compaction
      // has started, statusFileUpdation will be false , but at the same time commitComplete can be
      // true because compaction for all datamaps will be finished at a time to the maximum level
      // possible (level 1, 2 etc). so we need to check for either condition
      if (!statusFileUpdation || !commitComplete) {
        LOGGER.error(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        throw new Exception(s"Compaction failed to update metadata for table" +
                            s" ${ carbonLoadModel.getDatabaseName }." +
                            s"${ carbonLoadModel.getTableName }")
      } else {
        LOGGER.info(s"Compaction request completed for table " +
                    s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      }
    } else {
      LOGGER.error(s"Compaction request failed for table " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      throw new Exception("Compaction Failure in Merger Rdd.")
    }
  }

  def globalSortForMinor(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      carbonMergerMapping: CarbonMergerMapping): Array[(String, Boolean)] = {
    val dataFrame = SegmentUtils.dataFrameOfSegments(
      sparkSession,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
      carbonMergerMapping.validSegments)
    val outputModel = SegmentUtils.getLoadModel(
      sparkSession, carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
      carbonMergerMapping.validSegments)
    outputModel.setSegmentId(carbonMergerMapping.mergedLoadName.split("_")(1))
    DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(
      sparkSession,
      Option(dataFrame),
      outputModel,
      sparkSession.sessionState.newHadoopConf()
    )
      .map { row =>
        (row._1, FailureCauses.NONE == row._2._2.failureCauses)
      }
  }
}
