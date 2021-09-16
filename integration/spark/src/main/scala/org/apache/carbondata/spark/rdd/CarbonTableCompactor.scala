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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.{CarbonThreadUtil, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, CarbonMergerMapping, CompactionCallableModel, CompactionModel}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.{CollectionAccumulator, MergeIndexUtil}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexStoreManager, Segment}
import org.apache.carbondata.core.indexstore.PartitionSpec
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
import org.apache.carbondata.trash.DataTrashManager
import org.apache.carbondata.view.MVManagerInSpark

/**
 * This class is used to perform compaction on carbon table.
 */
class CarbonTableCompactor(
    carbonLoadModel: CarbonLoadModel,
    compactionModel: CompactionModel,
    sqlContext: SQLContext,
    compactedSegments: List[String],
    operationContext: OperationContext)
  extends Compactor(carbonLoadModel, compactionModel) {

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
      val compactedLoad = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
      var segmentLocks: ListBuffer[ICarbonLock] = ListBuffer.empty
      val validSegments = new java.util.ArrayList[LoadMetadataDetails]
      loadsToMerge.asScala.foreach { segmentId =>
        val segmentLock = CarbonLockFactory
          .getCarbonLockObj(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
            .getAbsoluteTableIdentifier,
            CarbonTablePath.addSegmentPrefix(segmentId.getLoadName) + LockUsage.LOCK)
        if (segmentLock.lockWithRetries()) {
          validSegments.add(segmentId)
          segmentLocks += segmentLock
        } else {
          LOGGER.warn(s"Failed to acquire lock on segment ${segmentId.getLoadName}, " +
                      s"during compaction of table ${compactionModel.carbonTable.getQualifiedName}")
        }
      }
      try {
        // need to be cleared for multiple compactions.
        // only contains the segmentIds which have to be compacted.
        compactedSegments.clear()
        scanSegmentsAndSubmitJob(validSegments, compactedSegments, compactedLoad)
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
    try {
      triggerCompaction(compactionCallableModel, mergedLoadName: String)
    } catch {
      case e: Throwable =>
        // clean stale compaction segment immediately after compaction failure
        DataTrashManager.cleanStaleCompactionSegment(
          carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
          mergedLoadName.split(CarbonCommonConstants.UNDERSCORE)(1),
          carbonLoadModel.getFactTimeStamp,
          compactionCallableModel.compactedPartitions)
        throw e
    }
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

    val updatePartitionSpecs : List[PartitionSpec] = new util.ArrayList[PartitionSpec]
    var mergeRDD: CarbonMergerRDD[String, Boolean] = null
    if (carbonTable.isHivePartitionTable) {
      // collect related partitions
      mergeRDD = new CarbonMergerRDD(
        sc.sparkSession,
        new MergeResultImpl(),
        carbonLoadModel,
        carbonMergerMapping,
        segmentMetaDataAccumulator
      )
      val partitionSpecs = mergeRDD.getPartitions.map { partition =>
        partition.asInstanceOf[CarbonSparkPartition].partitionSpec.get
      }.distinct
      if (partitionSpecs != null && partitionSpecs.nonEmpty) {
        compactionCallableModel.compactedPartitions = Some(partitionSpecs)
      }
      partitionSpecs.foreach(partitionSpec => {
        if (!partitionSpec.getLocation.toString.startsWith(carbonLoadModel.getTablePath)) {
          // if partition spec added is external path,
          // after compaction location path to be updated with table path.
          updatePartitionSpecs.add(partitionSpec)
        }
      })
    }

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
        if (mergeRDD != null) {
          val result = mergeRDD.collect
          if (!updatePartitionSpecs.isEmpty) {
            val tableIdentifier = new TableIdentifier(carbonTable.getTableName,
              Some(carbonTable.getDatabaseName))
            val partitionSpecs = updatePartitionSpecs.asScala.map {
              partitionSpec =>
                // replaces old partitionSpec with updated partitionSpec
                mergeRDD.checkAndUpdatePartitionLocation(partitionSpec)
                PartitioningUtils.parsePathFragment(
                  String.join(CarbonCommonConstants.FILE_SEPARATOR, partitionSpec.getPartitions))
            }
            // To update partitionSpec in hive metastore, drop and add with latest path.
            AlterTableDropPartitionCommand(
              tableIdentifier,
              partitionSpecs,
              true, false, true).run(sqlContext.sparkSession)
            AlterTableAddPartitionCommand(tableIdentifier,
              partitionSpecs.map(p => (p, None)), false).run(sqlContext.sparkSession)
          }
          result
        } else {
          new CarbonMergerRDD(
            sc.sparkSession,
            new MergeResultImpl(),
            carbonLoadModel,
            carbonMergerMapping,
            segmentMetaDataAccumulator
          ).collect
        }
      }

    if (mergeStatus.length == 0) {
      finalMergeStatus = false
    } else {
      finalMergeStatus = mergeStatus.forall(_._2)
    }

    if (finalMergeStatus) {
      val mergedLoadNumber = CarbonDataMergerUtil.getLoadNumberFromLoadName(mergedLoadName)
      var segmentFileName: String = null

      val isMergeIndex = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
        CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT).toBoolean

      if (compactionType != CompactionType.IUD_DELETE_DELTA && isMergeIndex) {
        MergeIndexUtil.mergeIndexFilesOnCompaction(compactionCallableModel)
      }

      if (carbonTable.isHivePartitionTable) {
        if (isMergeIndex) {
          val segmentTmpFileName = carbonLoadModel.getFactTimeStamp + CarbonTablePath.SEGMENT_EXT
          segmentFileName = mergedLoadNumber + CarbonCommonConstants.UNDERSCORE + segmentTmpFileName
          val segmentTmpFile = FileFactory.getCarbonFile(
            CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath, segmentTmpFileName))
          if (!segmentTmpFile.renameForce(
            CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath, segmentFileName))) {
            throw new Exception(s"Rename segment file from ${segmentTmpFileName} " +
              s"to ${segmentFileName} failed.")
          }
        } else {
          // By default carbon.merge.index.in.segment is true and this code will be used for
          // developer debugging purpose.
          val readPath =
            CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath) +
              CarbonCommonConstants.FILE_SEPARATOR + carbonLoadModel.getFactTimeStamp + ".tmp"
          // Merge all partition files into a single file.
          segmentFileName =
            mergedLoadNumber + CarbonCommonConstants.UNDERSCORE + carbonLoadModel.getFactTimeStamp
          val mergedSegmetFile = SegmentFileStore
            .mergeSegmentFiles(readPath,
              segmentFileName,
              CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath))
          if (mergedSegmetFile != null) {
            SegmentFileStore
              .moveFromTempFolder(mergedSegmetFile,
                carbonLoadModel.getFactTimeStamp + ".tmp",
                carbonLoadModel.getTablePath)
          }
          segmentFileName = segmentFileName + CarbonTablePath.SEGMENT_EXT
        }
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
        LOGGER.error(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        throw new Exception(s"Compaction failed to update metadata for table" +
                            s" ${ carbonLoadModel.getDatabaseName }." +
                            s"${ carbonLoadModel.getTableName }")
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
      CarbonThreadUtil
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
        segmentMetaDataAccumulator,
        isCompactionFlow = true)
        .map { row =>
          (row._1, FailureCauses.NONE == row._2._2.failureCauses)
        }
    } finally {
      CarbonThreadUtil
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
