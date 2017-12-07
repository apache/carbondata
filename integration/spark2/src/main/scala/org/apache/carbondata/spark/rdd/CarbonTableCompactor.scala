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
import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel, CompactionModel}

import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.events.{AlterTableCompactionPostEvent, AlterTableCompactionPreEvent, OperationContext, OperationListenerBus}
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
    storeLocation: String)
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
      compactionModel.compactionType)
    triggerCompaction(compactionCallableModel)
  }

  private def triggerCompaction(compactionCallableModel: CompactionCallableModel): Unit = {
    val carbonTable = compactionCallableModel.carbonTable
    val loadsToMerge = compactionCallableModel.loadsToMerge
    val sc = compactionCallableModel.sqlContext
    val carbonLoadModel = compactionCallableModel.carbonLoadModel
    val compactionType = compactionCallableModel.compactionType
    val tablePath = carbonLoadModel.getTablePath
    val startTime = System.nanoTime()
    val mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
    var finalMergeStatus = false
    val databaseName: String = carbonLoadModel.getDatabaseName
    val factTableName = carbonLoadModel.getTableName
    val validSegments: Array[String] = CarbonDataMergerUtil
      .getValidSegments(loadsToMerge).split(',')
    val mergeLoadStartTime = CarbonUpdateUtil.readCurrentTime()
    val carbonMergerMapping = CarbonMergerMapping(tablePath,
      carbonTable.getMetaDataFilepath,
      mergedLoadName,
      databaseName,
      factTableName,
      validSegments,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
      compactionType,
      maxSegmentColCardinality = null,
      maxSegmentColumnSchemaList = null
    )
    carbonLoadModel.setTablePath(carbonMergerMapping.hdfsStoreLocation)
    carbonLoadModel.setLoadMetadataDetails(
      SegmentStatusManager.readLoadMetadata(carbonTable.getMetaDataFilepath).toList.asJava)
    // trigger event for compaction
    val operationContext = new OperationContext
    val alterTableCompactionPreEvent: AlterTableCompactionPreEvent =
      AlterTableCompactionPreEvent(carbonTable, carbonMergerMapping, mergedLoadName, sc)
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
      CommonUtil.mergeIndexFiles(
        sc.sparkContext, Seq(mergedLoadNumber), tablePath, carbonTable, false)

      // trigger event for compaction
      val alterTableCompactionPostEvent: AlterTableCompactionPostEvent =
        AlterTableCompactionPostEvent(carbonTable, carbonMergerMapping, mergedLoadName, sc)
      OperationListenerBus.getInstance.fireEvent(alterTableCompactionPostEvent, operationContext)

      val endTime = System.nanoTime()
      LOGGER.info(s"time taken to merge $mergedLoadName is ${ endTime - startTime }")
      val statusFileUpdation =
        ((compactionType == CompactionType.IUD_UPDDEL_DELTA) &&
         CarbonDataMergerUtil
           .updateLoadMetadataIUDUpdateDeltaMergeStatus(loadsToMerge,
             carbonTable.getMetaDataFilepath,
             carbonLoadModel)) ||
        CarbonDataMergerUtil
          .updateLoadMetadataWithMergeStatus(loadsToMerge, carbonTable.getMetaDataFilepath,
            mergedLoadNumber, carbonLoadModel, mergeLoadStartTime, compactionType)

      if (!statusFileUpdation) {
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
