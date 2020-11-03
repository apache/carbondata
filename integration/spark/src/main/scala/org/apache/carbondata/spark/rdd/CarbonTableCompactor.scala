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
import java.util.{Collections, List}
import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.{CarbonUtils, SparkSession, SQLContext}
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel, CompactionModel}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.{CollectionAccumulator, MergeIndexUtil}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexStoreManager, Segment}
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.indexserver.{DistributedRDDUtils, IndexServer}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CarbonCompactionUtil, CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.MergeResultImpl
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark
import org.apache.carbondata.spark.util.CarbonSparkUtil
import org.apache.carbondata.view.MVManagerInSpark

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
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable, loadsToMerge.get(0))
    } else {
      false
    }
  }

  override def executeCompaction(): Unit = {
    val sortedSegments: util.List[LoadMetadataDetails] = new util.ArrayList[LoadMetadataDetails](
      carbonLoadModel.getLoadMetadataDetails.asScala.filter(_.isCarbonFormat).asJava
    )
    CarbonDataMergerUtil.sortSegments(sortedSegments)

    var loadsToMerge = identifySegmentsToBeMerged()

    while (loadsToMerge.size() > 1 || needSortSingleSegment(loadsToMerge)) {
      val lastSegment = sortedSegments.get(sortedSegments.size() - 1)
      deletePartialLoadsInCompaction()
      val compactedLoad = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
      var segmentLocks: ListBuffer[ICarbonLock] = ListBuffer.empty
      loadsToMerge.asScala.foreach { segmentId =>
        val segmentLock = CarbonLockFactory
          .getCarbonLockObj(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
            .getAbsoluteTableIdentifier,
            CarbonTablePath.addSegmentPrefix(segmentId.getLoadName) + LockUsage.LOCK)
        segmentLock.lockWithRetries()
        segmentLocks += segmentLock
      }
      try {
        scanSegmentsAndSubmitJob(loadsToMerge, compactedSegments, compactedLoad)
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in compaction thread ${ e.getMessage }", e)
          // in case of exception, clear the cache loaded both in driver, and index server if
          // enabled. Consider a scenario where listener is called for SI table to do load after
          // compaction, then basically SI loads the new compacted load of main table to cache as it
          // needs to select data from main table. after that if the load to SI fails, cache is to
          // be cleared.
          val compactedLoadToClear = compactedLoad.substring(
            compactedLoad.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1)
          if (!CarbonProperties.getInstance()
            .isDistributedPruningEnabled(carbonLoadModel.getDatabaseName,
              carbonLoadModel.getTableName)) {
            IndexStoreManager.getInstance()
              .clearInvalidSegments(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
                Collections.singletonList(compactedLoadToClear))
          } else {
            IndexServer.getClient
              .invalidateSegmentCache(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
                Array(compactedLoadToClear))
          }
          throw e
      } finally {
        segmentLocks.foreach { segmentLock =>
          segmentLock.unlock()
        }
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

      if (CompactionType.CUSTOM == compactionModel.compactionType) {
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
      compactedSegments: List[String], mergedLoadName: String): Unit = {
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
    triggerCompaction(compactionCallableModel, mergedLoadName: String)
  }

  private def triggerCompaction(compactionCallableModel: CompactionCallableModel,
      mergedLoadName: String): Unit = {
    val carbonTable = compactionCallableModel.carbonTable
    val loadsToMerge = compactionCallableModel.loadsToMerge
    val sc = compactionCallableModel.sqlContext
    val carbonLoadModel = compactionCallableModel.carbonLoadModel
    val compactionType = compactionCallableModel.compactionType
    val partitions = compactionCallableModel.currentPartitions
    val tablePath = carbonLoadModel.getTablePath
    val startTime = System.nanoTime()
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
    // Add pre event listener for index indexSchema
    val tableIndexes = IndexStoreManager.getInstance().getAllCGAndFGIndexes(carbonTable)
    val indexOperationContext = new OperationContext()
    if (null != tableIndexes) {
      val indexNames: mutable.Buffer[String] =
        tableIndexes.asScala.map(index => index.getIndexSchema.getIndexName)
      val indexPreExecutionEvent: BuildIndexPreExecutionEvent =
        new BuildIndexPreExecutionEvent(sqlContext.sparkSession,
        carbonTable.getAbsoluteTableIdentifier, indexNames)
      OperationListenerBus.getInstance().fireEvent(indexPreExecutionEvent,
        indexOperationContext)
    }
    // accumulator to collect segment metadata
    val segmentMetaDataAccumulator = sqlContext
      .sparkContext
      .collectionAccumulator[Map[String, SegmentMetaDataInfo]]

    val mergeStatus =
      if (SortScope.GLOBAL_SORT == carbonTable.getSortScope &&
          !carbonTable.getSortColumns.isEmpty &&
          carbonTable.getRangeColumn == null &&
          CarbonUtil.isStandardCarbonTable(carbonTable)) {
        compactSegmentsByGlobalSort(sc.sparkSession,
          carbonLoadModel,
          carbonMergerMapping,
          segmentMetaDataAccumulator)
      } else {
        new CarbonMergerRDD(
          sc.sparkSession,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping,
          segmentMetaDataAccumulator
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
        val segmentMetaDataInfo = CommonLoadUtils.getSegmentMetaDataInfoFromAccumulator(
          mergedLoadNumber,
          segmentMetaDataAccumulator)
        segmentFileName = SegmentFileStore.writeSegmentFile(
          carbonTable,
          mergedLoadNumber,
          carbonLoadModel.getFactTimeStamp.toString,
          segmentMetaDataInfo)
      }
      // clear segmentMetaDataAccumulator
      segmentMetaDataAccumulator.reset()
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
      val statusFileUpdate =
        CarbonDataMergerUtil.updateLoadMetadataWithMergeStatus(
          loadsToMerge,
          carbonTable.getMetadataPath,
          mergedLoadNumber,
          carbonLoadModel,
          compactionType,
          segmentFileName,
          MVManagerInSpark.get(sc.sparkSession))

      if (!statusFileUpdate) {
        // no need to call merge index if table status update has failed
        LOGGER.error(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        throw new Exception(s"Compaction failed to update metadata for table" +
                            s" ${ carbonLoadModel.getDatabaseName }." +
                            s"${ carbonLoadModel.getTableName }")
      }

      if (compactionType != CompactionType.IUD_DELETE_DELTA) {
        MergeIndexUtil.mergeIndexFilesOnCompaction(compactionCallableModel)
      }

      val compactionLoadStatusPostEvent = AlterTableCompactionPostStatusUpdateEvent(sc.sparkSession,
        carbonTable,
        carbonMergerMapping,
        carbonLoadModel,
        mergedLoadName)
      OperationListenerBus.getInstance()
        .fireEvent(compactionLoadStatusPostEvent, operationContext)
      if (null != tableIndexes) {
        val buildIndexPostExecutionEvent = new BuildIndexPostExecutionEvent(
          sqlContext.sparkSession, carbonTable.getAbsoluteTableIdentifier,
          null, Seq(mergedLoadNumber), true)
        OperationListenerBus.getInstance()
          .fireEvent(buildIndexPostExecutionEvent, indexOperationContext)
      }
      val commitDone = operationContext.getProperty("commitComplete")
      val commitComplete = if (null != commitDone) {
        commitDone.toString.toBoolean
      } else {
        true
      }
      if (!commitComplete) {
        LOGGER.error(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        throw new Exception(s"Compaction failed to update metadata for table" +
                            s" ${ carbonLoadModel.getDatabaseName }." +
                            s"${ carbonLoadModel.getTableName }")
      } else {
        LOGGER.info(s"Compaction request completed for table " +
                    s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")

        // Pre-priming index for compaction
        val segmentsForPriming = if (compactionType.equals(CompactionType.IUD_DELETE_DELTA)) {
            validSegments.asScala.map(_.getSegmentNo).toList
        } else if (compactionType.equals(CompactionType.MAJOR) ||
                   compactionType.equals(CompactionType.MINOR) ||
                   compactionType.equals(CompactionType.CUSTOM)) {
            scala.List(mergedLoadNumber)
        } else {
          scala.List()
        }
        DistributedRDDUtils.triggerPrepriming(sqlContext.sparkSession,
          carbonTable,
          validSegments.asScala.map(_.getSegmentNo).toList,
          operationContext,
          FileFactory.getConfiguration,
          segmentsForPriming)
      }
    } else {
      LOGGER.error(s"Compaction request failed for table " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      throw new Exception("Compaction Failure in Merger Rdd.")
    }
  }

  /**
   * compact segments by global sort
   */
  def compactSegmentsByGlobalSort(
      sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      carbonMergerMapping: CarbonMergerMapping,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]])
  : Array[(String, Boolean)] = {
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val splits = splitsOfSegments(
      sparkSession,
      table,
      carbonMergerMapping.validSegments)
    var loadResult: Array[(String, Boolean)] = null
    try {
      CarbonUtils
        .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
          table.getDatabaseName + CarbonCommonConstants.POINT + table.getTableName,
          splits.asScala.map(s => s.asInstanceOf[CarbonInputSplit].getSegmentId).mkString(","))
      val dataFrame = SparkSQLUtil.createInputDataFrame(
        sparkSession,
        table)

      // generate LoadModel which can be used global_sort flow
      val outputModel = DataLoadProcessBuilderOnSpark.createLoadModelForGlobalSort(
        sparkSession, table)
      // set fact time stamp, else the carbondata file will be created with fact timestamp as 0.
      outputModel.setFactTimeStamp(carbonLoadModel.getFactTimeStamp)
      outputModel.setLoadMetadataDetails(carbonLoadModel.getLoadMetadataDetails)
      outputModel.setSegmentId(carbonMergerMapping.mergedLoadName.split("_")(1))
      loadResult = DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(
        sparkSession,
        Option(dataFrame),
        outputModel,
        SparkSQLUtil.sessionState(sparkSession).newHadoopConf(),
        segmentMetaDataAccumulator)
        .map { row =>
          (row._1, FailureCauses.NONE == row._2._2.failureCauses)
        }
    } finally {
      CarbonUtils
        .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
          table.getDatabaseName + "." +
          table.getTableName)
    }
    loadResult
  }

  /**
   * get splits of specified segments
   */
  def splitsOfSegments(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      segments: Array[Segment]
  ): java.util.List[InputSplit] = {
    val job = CarbonSparkUtil.createHadoopJob()
    val conf = job.getConfiguration
    CarbonInputFormat.setTablePath(conf, carbonTable.getTablePath)
    CarbonInputFormat.setTableInfo(conf, carbonTable.getTableInfo)
    CarbonInputFormat.setDatabaseName(conf, carbonTable.getDatabaseName)
    CarbonInputFormat.setTableName(conf, carbonTable.getTableName)
    CarbonInputFormat.setQuerySegment(conf, segments.map(_.getSegmentNo).mkString(","))
    new CarbonTableInputFormat[Object].getSplits(job)
  }

}
