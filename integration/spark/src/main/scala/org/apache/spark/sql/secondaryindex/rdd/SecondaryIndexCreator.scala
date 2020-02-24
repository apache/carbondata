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

package org.apache.spark.sql.secondaryindex.rdd

import java.util
import java.util.concurrent.Callable

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.{CarbonEnv, SQLContext}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.command.SecondaryIndexModel
import org.apache.spark.sql.secondaryindex.events.{LoadTableSIPostExecutionEvent, LoadTableSIPreExecutionEvent}
import org.apache.spark.sql.secondaryindex.util.{CarbonInternalScalaUtil, FileInternalUtil, SecondaryIndexCreationResultImpl, SecondaryIndexUtil}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil

/**
 * This class is aimed at creating secondary index for specified segments
 */
object SecondaryIndexCreator {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def createSecondaryIndex(secondaryIndexModel: SecondaryIndexModel,
    segmentToLoadStartTimeMap: java.util.Map[String, String],
    indexTable: CarbonTable,
    forceAccessSegment: Boolean = false,
    isCompactionCall: Boolean,
    isLoadToFailedSISegments: Boolean): CarbonTable = {
    var indexCarbonTable = indexTable
    val sc = secondaryIndexModel.sqlContext
    // get the thread pool size for secondary index creation
    val threadPoolSize = getThreadPoolSize(sc)
    LOGGER
      .info(s"Configured thread pool size for distributing segments in secondary index creation " +
            s"is $threadPoolSize")
    // create executor service to parallely run the segments
    val executorService = java.util.concurrent.Executors.newFixedThreadPool(threadPoolSize)
    if (null == indexCarbonTable) {
      // avoid more lookupRelation to table
      val metastore = CarbonEnv.getInstance(secondaryIndexModel.sqlContext.sparkSession)
        .carbonMetaStore
      indexCarbonTable = metastore
        .lookupRelation(Some(secondaryIndexModel.carbonLoadModel.getDatabaseName),
          secondaryIndexModel.secondaryIndex.indexTableName)(secondaryIndexModel.sqlContext
          .sparkSession).asInstanceOf[CarbonRelation].carbonTable
    }

    val operationContext = new OperationContext
    val loadTableSIPreExecutionEvent: LoadTableSIPreExecutionEvent =
      LoadTableSIPreExecutionEvent(secondaryIndexModel.sqlContext.sparkSession,
        new CarbonTableIdentifier(indexCarbonTable.getDatabaseName,
          indexCarbonTable.getTableName, ""),
        secondaryIndexModel.carbonLoadModel,
        indexCarbonTable)
    OperationListenerBus.getInstance
      .fireEvent(loadTableSIPreExecutionEvent, operationContext)

    var segmentLocks: ListBuffer[ICarbonLock] = ListBuffer.empty
    val validSegments: java.util.List[String] = new util.ArrayList[String]()
    var validSegmentList = List.empty[String]

    try {
      for (eachSegment <- secondaryIndexModel.validSegments) {
        // take segment lock before starting the actual load, so that the clearing the invalid
        // segments in the parallel load to SI will not clear the current valid loading segment
        // and this new segment will not be added as failed segment in
        // SILoadEventListenerForFailedSegments as the failed segments list
        // is validated based on the segment lock
        val segmentLock = CarbonLockFactory
          .getCarbonLockObj(indexCarbonTable.getAbsoluteTableIdentifier,
            CarbonTablePath.addSegmentPrefix(eachSegment) + LockUsage.LOCK)
        if (segmentLock.lockWithRetries(1, 0)) {
          segmentLocks += segmentLock
          // add only the segments for which we are able to get segments lock and trigger
          // loading for these segments. if some segments are skipped,
          // skipped segments load will be handled in SILoadEventListenerForFailedSegments
          validSegments.add(eachSegment)
        } else {
          LOGGER.error(s"Not able to acquire the segment lock for table" +
                       s" ${indexCarbonTable.getTableUniqueName} for segment: $eachSegment. " +
                       s"Skipping this segment from loading.")
        }
      }

      validSegmentList = validSegments.asScala.toList

      LOGGER.info(s"${indexCarbonTable.getTableUniqueName}: SI loading is started " +
              s"for segments: $validSegmentList")

      FileInternalUtil
        .updateTableStatus(validSegmentList,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.INSERT_IN_PROGRESS,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          new java.util
          .HashMap[String,
            String](),
          indexCarbonTable,
          sc.sparkSession)
      var execInstance = "1"
      // in case of non dynamic executor allocation, number of executors are fixed.
      if (sc.sparkContext.getConf.contains("spark.executor.instances")) {
        execInstance = sc.sparkContext.getConf.get("spark.executor.instances")
        LOGGER.info("spark.executor.instances property is set to =" + execInstance)
      }
      // in case of dynamic executor allocation, taking the max executors
      // of the dynamic allocation.
      else if (sc.sparkContext.getConf.contains("spark.dynamicAllocation.enabled")) {
        if (sc.sparkContext.getConf.get("spark.dynamicAllocation.enabled").trim
          .equalsIgnoreCase("true")) {
          execInstance = sc.sparkContext.getConf.get("spark.dynamicAllocation.maxExecutors")
          LOGGER.info("spark.dynamicAllocation.maxExecutors property is set to =" + execInstance)
        }
      }
      var futureObjectList = List[java.util.concurrent.Future[Array[(String, Boolean)]]]()
      for (eachSegment <- validSegmentList) {
        val segId = eachSegment
        futureObjectList :+= executorService.submit(new Callable[Array[(String, Boolean)]] {
          @throws(classOf[Exception])
          override def call(): Array[(String, Boolean)] = {
            ThreadLocalSessionInfo.getOrCreateCarbonSessionInfo().getNonSerializableExtraInfo
              .put("carbonConf", SparkSQLUtil.sessionState(sc.sparkSession).newHadoopConf())
            var eachSegmentSecondaryIndexCreationStatus: Array[(String, Boolean)] = Array.empty
            CarbonLoaderUtil.checkAndCreateCarbonDataLocation(segId, indexCarbonTable)
            val carbonLoadModel = getCopyObject(secondaryIndexModel)
            carbonLoadModel
              .setFactTimeStamp(secondaryIndexModel.segmentIdToLoadStartTimeMapping(eachSegment))
            carbonLoadModel.setTablePath(secondaryIndexModel.carbonTable.getTablePath)
            val secondaryIndexCreationStatus = new CarbonSecondaryIndexRDD(sc.sparkSession,
              new SecondaryIndexCreationResultImpl,
              carbonLoadModel,
              secondaryIndexModel.secondaryIndex,
              segId, execInstance, indexCarbonTable, forceAccessSegment).collect()
            val segmentFileName =
              SegmentFileStore
                .writeSegmentFile(indexCarbonTable,
                  segId,
                  String.valueOf(carbonLoadModel.getFactTimeStamp))
            segmentToLoadStartTimeMap.put(segId, String.valueOf(carbonLoadModel.getFactTimeStamp))
            if (secondaryIndexCreationStatus.length > 0) {
              eachSegmentSecondaryIndexCreationStatus = secondaryIndexCreationStatus
            }
            eachSegmentSecondaryIndexCreationStatus
          }
        })
      }

      val segmentSecondaryIndexCreationStatus = futureObjectList.filter(_.get().length > 0)
        .groupBy(a => a.get().head._2)
      val hasSuccessSegments = segmentSecondaryIndexCreationStatus.contains("true".toBoolean)
      val hasFailedSegments = segmentSecondaryIndexCreationStatus.contains("false".toBoolean)
      var successSISegments: List[String] = List()
      var failedSISegments: List[String] = List()
      if (hasSuccessSegments) {
        successSISegments =
          segmentSecondaryIndexCreationStatus("true".toBoolean).collect {
            case segments: java.util.concurrent.Future[Array[(String, Boolean)]] =>
              segments.get().head._1
          }
      }

      if (hasFailedSegments) {
        // if the call is from compaction, we need to fail the main table compaction also, and if
        // the load is called from SIloadEventListener, which is for corresponding main table
        // segment, then if SI load fails, we need to fail main table load also, so throw exception,
        // if load is called from SI creation or SILoadEventListenerForFailedSegments, no need to
        // fail, just make the segement as marked for delete, so that next load to main table will
        // take care
        if (isCompactionCall || !isLoadToFailedSISegments) {
          throw new Exception("Secondary index creation failed")
        } else {
          failedSISegments =
            segmentSecondaryIndexCreationStatus("false".toBoolean).collect {
              case segments: java.util.concurrent.Future[Array[(String, Boolean)]] =>
                segments.get().head._1
            }
        }
      }
      // what and all segments the load failed, only for those need make status as marked
      // for delete, remaining let them be SUCCESS
      var tableStatusUpdateForSuccess = false
      var tableStatusUpdateForFailure = false

      if (successSISegments.nonEmpty && !isCompactionCall) {
        tableStatusUpdateForSuccess = FileInternalUtil.updateTableStatus(
          successSISegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.INSERT_IN_PROGRESS,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          segmentToLoadStartTimeMap,
          indexCarbonTable,
          secondaryIndexModel.sqlContext.sparkSession)

        // merge index files for success segments in case of only load
        CarbonMergeFilesRDD.mergeIndexFiles(secondaryIndexModel.sqlContext.sparkSession,
          successSISegments,
          segmentToLoadStartTimeMap,
          indexCarbonTable.getTablePath,
          indexCarbonTable, mergeIndexProperty = false)

        val loadMetadataDetails = SegmentStatusManager
          .readLoadMetadata(indexCarbonTable.getMetadataPath)
          .filter(loadMetadataDetail => successSISegments.contains(loadMetadataDetail.getLoadName))

        val carbonLoadModelForMergeDataFiles = SecondaryIndexUtil
          .getCarbonLoadModel(indexCarbonTable,
            loadMetadataDetails.toList.asJava,
            System.currentTimeMillis(),
            CarbonInternalScalaUtil
              .getCompressorForIndexTable(indexCarbonTable.getDatabaseName,
                indexCarbonTable.getTableName,
                secondaryIndexModel.carbonTable.getTableName)(sc.sparkSession))

        // merge the data files of the loaded segments and take care of
        // merging the index files inside this if needed
        val rebuiltSegments = SecondaryIndexUtil
          .mergeDataFilesSISegments(secondaryIndexModel.segmentIdToLoadStartTimeMapping,
            indexCarbonTable,
            loadMetadataDetails.toList.asJava, carbonLoadModelForMergeDataFiles)(sc)

        tableStatusUpdateForSuccess = FileInternalUtil.updateTableStatus(
          successSISegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.SUCCESS,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          segmentToLoadStartTimeMap,
          indexCarbonTable,
          secondaryIndexModel.sqlContext.sparkSession,
          carbonLoadModelForMergeDataFiles.getFactTimeStamp,
          rebuiltSegments)
      }

      // Index PrePriming for SI
      DistributedRDDUtils.triggerPrepriming(secondaryIndexModel.sqlContext.sparkSession,
        indexCarbonTable,
        Seq(),
        operationContext,
        FileFactory.getConfiguration,
        validSegments.asScala.toList)

      // update the status of all the segments to marked for delete if data load fails, so that
      // next load which is triggered for SI table in post event of main table data load clears
      // all the segments of marked for delete and retriggers the load to same segments again in
      // that event
      if (failedSISegments.nonEmpty && !isCompactionCall) {
        tableStatusUpdateForFailure = FileInternalUtil.updateTableStatus(
          failedSISegments,
          secondaryIndexModel.carbonLoadModel.getDatabaseName,
          secondaryIndexModel.secondaryIndex.indexTableName,
          SegmentStatus.MARKED_FOR_DELETE,
          secondaryIndexModel.segmentIdToLoadStartTimeMapping,
          segmentToLoadStartTimeMap,
          indexCarbonTable,
          secondaryIndexModel.sqlContext.sparkSession)
      }

      if (failedSISegments.nonEmpty) {
        LOGGER.error("Dataload to secondary index creation has failed")
      }

      val loadTableSIPostExecutionEvent: LoadTableSIPostExecutionEvent =
        LoadTableSIPostExecutionEvent(sc.sparkSession,
          indexCarbonTable.getCarbonTableIdentifier,
          secondaryIndexModel.carbonLoadModel,
          indexCarbonTable)
      OperationListenerBus.getInstance
        .fireEvent(loadTableSIPostExecutionEvent, operationContext)

      indexCarbonTable
    } catch {
      case ex: Exception =>
        FileInternalUtil
          .updateTableStatus(validSegmentList,
            secondaryIndexModel.carbonLoadModel.getDatabaseName,
            secondaryIndexModel.secondaryIndex.indexTableName,
            SegmentStatus.MARKED_FOR_DELETE,
            secondaryIndexModel.segmentIdToLoadStartTimeMapping,
            new java.util
            .HashMap[String,
              String](),
            indexCarbonTable,
            sc.sparkSession)
        LOGGER.error(ex)
        throw ex
    } finally {
      // release the segment locks
      segmentLocks.foreach(segmentLock => {
        segmentLock.unlock()
      })
      try {
        if (!isCompactionCall) {
          SegmentStatusManager
            .deleteLoadsAndUpdateMetadata(indexCarbonTable, false, null)
          TableProcessingOperations.deletePartialLoadDataIfExist(indexCarbonTable, false)
        }
      } catch {
        case e: Exception =>
          LOGGER
            .error("Problem while cleaning up stale folder for index table " +
                   secondaryIndexModel.secondaryIndex.indexTableName, e)
      }
      // close the executor service
      if (null != executorService) {
        executorService.shutdownNow()
      }
    }
  }

  /**
   * will return the copy object of the existing object
   *
   * @return
   */
  def getCopyObject(secondaryIndexModel: SecondaryIndexModel): CarbonLoadModel = {
    val carbonLoadModel = secondaryIndexModel.carbonLoadModel
    val copyObj = new CarbonLoadModel
    copyObj.setTableName(carbonLoadModel.getTableName)
    copyObj.setDatabaseName(carbonLoadModel.getDatabaseName)
    copyObj.setLoadMetadataDetails(carbonLoadModel.getLoadMetadataDetails)
    copyObj.setCarbonDataLoadSchema(carbonLoadModel.getCarbonDataLoadSchema)
    copyObj.setColumnCompressor(CarbonInternalScalaUtil
      .getCompressorForIndexTable(carbonLoadModel.getDatabaseName,
        secondaryIndexModel.secondaryIndex.indexTableName,
        carbonLoadModel.getTableName)(secondaryIndexModel.sqlContext.sparkSession))
    copyObj
  }

  /**
   * This method will get the configuration for thread pool size which will decide the numbe rof
   * segments to run in parallel for secondary index creation
   *
   */
  def getThreadPoolSize(sqlContext: SQLContext): Int = {
    var threadPoolSize: Int = 0
    try {
      threadPoolSize = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS,
          CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT).toInt
      if (threadPoolSize >
          CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_MAX) {
        threadPoolSize = CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_MAX
        LOGGER
          .info(s"Configured thread pool size for secondary index creation is greater than " +
                s"default parallelism. Therefore default value will be considered: $threadPoolSize")
      } else {
        val defaultThreadPoolSize =
          CarbonCommonConstants.CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT.toInt
        if (threadPoolSize < defaultThreadPoolSize) {
          threadPoolSize = defaultThreadPoolSize
          LOGGER
            .info(s"Configured thread pool size for secondary index creation is incorrect. " +
                  s" Therefore default value will be considered: $threadPoolSize")
        }
      }
    } catch {
      case nfe: NumberFormatException =>
        threadPoolSize = CarbonCommonConstants
          .CARBON_SECONDARY_INDEX_CREATION_THREADS_DEFAULT.toInt
        LOGGER
          .info(s"Configured thread pool size for secondary index creation is incorrect. " +
                s" Therefore default value will be considered: $threadPoolSize")
    }
    threadPoolSize
  }
}
