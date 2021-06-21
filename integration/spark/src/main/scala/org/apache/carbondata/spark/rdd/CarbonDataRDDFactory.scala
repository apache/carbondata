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

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionCoalescer, DataLoadWrapperRDD, RDD}
import org.apache.spark.sql.{CarbonEnv, DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.command.{CompactionModel, ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.{CarbonException, SparkSQLUtil}
import org.apache.spark.util.CollectionAccumulator

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.{CarbonCommonConstants, SortScopeOptions}
import org.apache.carbondata.core.datastore.block.{Distributable, TableBlockInfo}
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.index.{IndexStoreManager, Segment}
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, ColumnarFormatVersion, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.indexserver.{DistributedRDDUtils, IndexServer}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.csvinput.BlockDetails
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostStatusUpdateEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonCompactionUtil, CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.{DataLoadResultImpl, _}
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CarbonSparkUtil, CommonUtil, Util}
import org.apache.carbondata.view.MVManagerInSpark

/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 */
object CarbonDataRDDFactory {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def handleCompactionForSystemLocking(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      compactionType: CompactionType,
      carbonTable: CarbonTable,
      compactedSegments: java.util.List[String],
      compactionModel: CompactionModel,
      operationContext: OperationContext): Unit = {
    // taking system level lock at the mdt file location
    var configuredMdtPath = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER,
      CarbonCommonConstants.CARBON_UPDATE_SYNC_FOLDER_DEFAULT).trim

    configuredMdtPath = CarbonUtil.checkAndAppendFileSystemURIScheme(configuredMdtPath)
    val lock = CarbonLockFactory.getSystemLevelCarbonLockObj(
      configuredMdtPath + CarbonCommonConstants.FILE_SEPARATOR +
        CarbonCommonConstants.SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER,
      LockUsage.SYSTEM_LEVEL_COMPACTION_LOCK)

    if (lock.lockWithRetries()) {
      LOGGER.info(s"Acquired the compaction lock for table ${ carbonLoadModel.getDatabaseName }" +
          s".${ carbonLoadModel.getTableName }")
      try {
        startCompactionThreads(
          sqlContext,
          carbonLoadModel,
          compactionModel,
          lock,
          compactedSegments,
          operationContext
        )
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
          lock.unlock()
          // if the compaction is a blocking call then only need to throw the exception.
          if (compactionModel.isDDLTrigger) {
            throw e
          }
      }
    } else {
      LOGGER.error("Not able to acquire the compaction lock for table " +
          s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      CarbonCompactionUtil
          .createCompactionRequiredFile(carbonTable.getMetadataPath, compactionType)
      // throw exception only in case of DDL trigger.
      if (compactionModel.isDDLTrigger) {
        CarbonException.analysisException(
          s"Compaction is in progress, compaction request for table " +
            s"${carbonLoadModel.getDatabaseName}.${carbonLoadModel.getTableName}" +
            " is in queue.")
      } else {
        LOGGER.error("Compaction is in progress, compaction request for table " +
          s"${carbonLoadModel.getDatabaseName}.${carbonLoadModel.getTableName}" +
          " is in queue.")
      }
    }
  }

  def startCompactionThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      compactionModel: CompactionModel,
      compactionLock: ICarbonLock,
      compactedSegments: java.util.List[String],
      operationContext: OperationContext): Unit = {
    // update the updated table status.
    carbonLoadModel.readAndSetLoadMetadataDetails()

    val compactionThread = new Thread {
      override def run(): Unit = {
        val compactor = CompactionFactory.getCompactor(
          carbonLoadModel,
          compactionModel,
          sqlContext,
          compactedSegments,
          operationContext)
        try {
          // compaction status of the table which is triggered by the user.
          var triggeredCompactionStatus = false
          var exception: Exception = null
          try {
            compactor.executeCompaction()
            triggeredCompactionStatus = true
          } catch {
            case e: Exception =>
              LOGGER.error(s"Exception in compaction thread ${ e.getMessage }")
              exception = e
          }
          // continue in case of exception also, check for all the tables.
          val isConcurrentCompactionAllowed = CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
            CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
          ).equalsIgnoreCase("true")

          if (!isConcurrentCompactionAllowed) {
            LOGGER.info("System level compaction lock is enabled.")
            val skipCompactionTables = ListBuffer[CarbonTableIdentifier]()
            var tableForCompaction = CarbonCompactionUtil.getNextTableToCompact(
              CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore
                .listAllTables(sqlContext.sparkSession).toArray,
              skipCompactionTables.toList.asJava)
            while (null != tableForCompaction) {
              LOGGER.info("Compaction request has been identified for table " +
                  s"${ tableForCompaction.getDatabaseName }." +
                  s"${ tableForCompaction.getTableName}")
              val table: CarbonTable = tableForCompaction
              val metadataPath = table.getMetadataPath
              val compactionType = CarbonCompactionUtil.determineCompactionType(metadataPath)

              val newCarbonLoadModel = prepareCarbonLoadModel(table)

              val compactionSize = CarbonDataMergerUtil
                .getCompactionSize(CompactionType.MAJOR, carbonLoadModel)

              val newCompactionModel = CompactionModel(
                compactionSize,
                compactionType,
                table,
                compactionModel.isDDLTrigger,
                CarbonFilters.getCurrentPartitions(sqlContext.sparkSession,
                  TableIdentifier(table.getTableName,
                  Some(table.getDatabaseName))), None)
              // proceed for compaction
              try {
                CompactionFactory.getCompactor(
                  newCarbonLoadModel,
                  newCompactionModel,
                  sqlContext,
                  compactedSegments,
                  operationContext).executeCompaction()
              } catch {
                case e: Exception =>
                  LOGGER.error("Exception in compaction thread for table " +
                      s"${ tableForCompaction.getDatabaseName }." +
                      s"${ tableForCompaction.getTableName }")
                // not handling the exception. only logging as this is not the table triggered
                // by user.
              } finally {
                // delete the compaction required file in case of failure or success also.
                if (!CarbonCompactionUtil
                    .deleteCompactionRequiredFile(metadataPath, compactionType)) {
                  // if the compaction request file is not been able to delete then
                  // add those tables details to the skip list so that it wont be considered next.
                  skipCompactionTables.+=:(tableForCompaction.getCarbonTableIdentifier)
                  LOGGER.error("Compaction request file can not be deleted for table " +
                      s"${ tableForCompaction.getDatabaseName }." +
                      s"${ tableForCompaction.getTableName }")
                }
              }
              // ********* check again for all the tables.
              tableForCompaction = CarbonCompactionUtil.getNextTableToCompact(
                CarbonEnv.getInstance(sqlContext.sparkSession).carbonMetaStore
                  .listAllTables(sqlContext.sparkSession).toArray,
                skipCompactionTables.asJava)
            }
          }
          // Remove compacted segments from executor cache.
          if (CarbonProperties.getInstance().isDistributedPruningEnabled(
              carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)) {
            try {
              IndexServer.getClient.invalidateSegmentCache(carbonLoadModel
                .getCarbonDataLoadSchema.getCarbonTable,
                compactedSegments.asScala.toArray,
                SparkSQLUtil.getTaskGroupId(sqlContext.sparkSession))
            } catch {
              case ex: Exception =>
                LOGGER.warn(s"Clear cache job has failed for ${carbonLoadModel
                  .getDatabaseName}.${carbonLoadModel.getTableName}", ex)
            }
          }
          // giving the user his error for telling in the beeline if his triggered table
          // compaction is failed.
          if (!triggeredCompactionStatus) {
            throw new Exception("Exception in compaction " + exception.getMessage)
          }
        } finally {
          compactionLock.unlock()
        }
      }
    }
    // calling the run method of a thread to make the call as blocking call.
    // in the future we may make this as concurrent.
    compactionThread.run()
  }

  private def prepareCarbonLoadModel(
      table: CarbonTable
  ): CarbonLoadModel = {
    val loadModel = new CarbonLoadModel
    loadModel.setTableName(table.getTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    loadModel.setCarbonDataLoadSchema(dataLoadSchema)
    loadModel.setTableName(table.getCarbonTableIdentifier.getTableName)
    loadModel.setDatabaseName(table.getCarbonTableIdentifier.getDatabaseName)
    loadModel.setTablePath(table.getTablePath)
    loadModel.setCarbonTransactionalTable(table.isTransactionalTable)
    loadModel.readAndSetLoadMetadataDetails()
    val loadStartTime = CarbonUpdateUtil.readCurrentTime()
    loadModel.setFactTimeStamp(loadStartTime)
    val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.COMPRESSOR,
        CompressorFactory.getInstance().getCompressor.getName)
    loadModel.setColumnCompressor(columnCompressor)
    loadModel
  }

  def loadCarbonData(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      partitionStatus: SegmentStatus = SegmentStatus.SUCCESS,
      overwriteTable: Boolean,
      hadoopConf: Configuration,
      dataFrame: Option[DataFrame] = None,
      scanResultRdd : Option[RDD[InternalRow]] = None,
      updateModel: Option[UpdateTableModel] = None,
      operationContext: OperationContext): LoadMetadataDetails = {
    // Check if any load need to be deleted before loading new data
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var status: Array[(String, (LoadMetadataDetails, ExecutionErrors))] = null
    var res: Array[List[(String, (LoadMetadataDetails, ExecutionErrors))]] = null
    // accumulator to collect segment metadata
    val segmentMetaDataAccumulator = sqlContext
      .sparkContext
      .collectionAccumulator[Map[String, SegmentMetaDataInfo]]
    // create new segment folder  in carbon store
    if (updateModel.isEmpty && carbonLoadModel.isCarbonTransactionalTable ||
        updateModel.isDefined) {
      CarbonLoaderUtil.checkAndCreateCarbonDataLocation(carbonLoadModel.getSegmentId, carbonTable)
    }
    var loadStatus = SegmentStatus.SUCCESS
    var errorMessage: String = "DataLoad failure"
    var executorMessage: String = ""
    val isSortTable = carbonTable.getNumberOfSortColumns > 0
    val sortScope = CarbonDataProcessorUtil.getSortScope(carbonLoadModel.getSortScope)

    val segmentLock = CarbonLockFactory.getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
      CarbonTablePath.addSegmentPrefix(carbonLoadModel.getSegmentId) + LockUsage.LOCK)

    // dataFrame.get.rdd.isEmpty() will launch a job, so avoid calling it multiple times
    val isEmptyDataframe = updateModel.isDefined && dataFrame.get.rdd.isEmpty()
    try {
      if (!carbonLoadModel.isCarbonTransactionalTable || segmentLock.lockWithRetries()) {
        if (isEmptyDataframe) {
          // if the rowToBeUpdated is empty, mark created segment as marked for delete and return
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, "")
        } else {
          status = if (scanResultRdd.isDefined) {
            val colSchema = carbonLoadModel
              .getCarbonDataLoadSchema
              .getCarbonTable
              .getTableInfo
              .getFactTable
              .getListOfColumns
              .asScala
              .filterNot(col => col.isInvisible || col.isComplexColumn)
            val convertedRdd = CommonLoadUtils.getConvertedInternalRow(
              colSchema,
              scanResultRdd.get,
              isGlobalSortPartition = false)
            if (isSortTable && sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT) &&
                !carbonLoadModel.isNonSchemaColumnsPresent) {
              DataLoadProcessBuilderOnSpark.insertDataUsingGlobalSortWithInternalRow(sqlContext
                .sparkSession,
                convertedRdd,
                carbonLoadModel,
                hadoopConf,
                segmentMetaDataAccumulator)
            } else if (sortScope.equals(SortScopeOptions.SortScope.NO_SORT)) {
              loadDataFrameForNoSort(sqlContext,
                None,
                Some(convertedRdd),
                carbonLoadModel,
                segmentMetaDataAccumulator)
            } else {
              loadDataFrame(sqlContext,
                None,
                Some(convertedRdd),
                carbonLoadModel,
                segmentMetaDataAccumulator)
            }
          } else {
            if (dataFrame.isEmpty && isSortTable &&
                carbonLoadModel.getRangePartitionColumn != null &&
                (sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT) ||
                 sortScope.equals(SortScopeOptions.SortScope.LOCAL_SORT))) {
              DataLoadProcessBuilderOnSpark
                .loadDataUsingRangeSort(sqlContext.sparkSession,
                  carbonLoadModel,
                  hadoopConf,
                  segmentMetaDataAccumulator)
            } else if (isSortTable && sortScope.equals(SortScopeOptions.SortScope.GLOBAL_SORT)) {
              DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(sqlContext.sparkSession,
                dataFrame,
                carbonLoadModel,
                hadoopConf,
                segmentMetaDataAccumulator)
            } else if (dataFrame.isDefined) {
              loadDataFrame(sqlContext,
                dataFrame,
                None,
                carbonLoadModel,
                segmentMetaDataAccumulator)
            } else {
              loadDataFile(sqlContext, carbonLoadModel, hadoopConf, segmentMetaDataAccumulator)
            }
          }
          val newStatusMap = scala.collection.mutable.Map.empty[String, SegmentStatus]
          if (status.nonEmpty) {
            status.foreach { eachLoadStatus =>
              val state = newStatusMap.get(eachLoadStatus._1)
              state match {
                case Some(SegmentStatus.LOAD_FAILURE) =>
                  newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2._1.getSegmentStatus)
                case Some(SegmentStatus.LOAD_PARTIAL_SUCCESS)
                  if eachLoadStatus._2._1.getSegmentStatus ==
                     SegmentStatus.SUCCESS =>
                  newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2._1.getSegmentStatus)
                case _ =>
                  newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2._1.getSegmentStatus)
              }
            }

            newStatusMap.foreach {
              case (key, value) =>
                if (value == SegmentStatus.LOAD_FAILURE) {
                  loadStatus = SegmentStatus.LOAD_FAILURE
                } else if (value == SegmentStatus.LOAD_PARTIAL_SUCCESS &&
                           loadStatus != SegmentStatus.LOAD_FAILURE) {
                  loadStatus = SegmentStatus.LOAD_PARTIAL_SUCCESS
                }
            }
          } else {
            // if no value is there in data load, make load status Success
            // and data load flow executes
            if ((dataFrame.isDefined || scanResultRdd.isDefined) && updateModel.isEmpty) {
              if (dataFrame.isDefined) {
                val rdd = dataFrame.get.rdd
                if (rdd.partitions == null || rdd.partitions.length == 0) {
                  LOGGER.warn("DataLoading finished. No data was loaded.")
                  loadStatus = SegmentStatus.SUCCESS
                }
              } else {
                if (scanResultRdd.get.partitions == null ||
                    scanResultRdd.get.partitions.length == 0) {
                  LOGGER.warn("DataLoading finished. No data was loaded.")
                  loadStatus = SegmentStatus.SUCCESS
                }
              }
            } else {
              loadStatus = SegmentStatus.LOAD_FAILURE
            }
          }

          if (loadStatus != SegmentStatus.LOAD_FAILURE &&
              partitionStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS) {
            loadStatus = partitionStatus
          }
        }
      }
    } catch {
      case ex: Throwable =>
        loadStatus = SegmentStatus.LOAD_FAILURE
        val (extrMsgLocal, errorMsgLocal) = CarbonScalaUtil.retrieveAndLogErrorMsg(ex, LOGGER)
        executorMessage = extrMsgLocal
        errorMessage = errorMsgLocal
        LOGGER.info(errorMessage)
        LOGGER.error(ex)
    }
    var isLoadingCommitted = false
    try {
      val uniqueTableStatusId = Option(operationContext.getProperty("uuid")).getOrElse("")
        .asInstanceOf[String]
      if (loadStatus == SegmentStatus.LOAD_FAILURE) {
        // update the load entry in table status file for changing the status to marked for delete
        CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uniqueTableStatusId)
        LOGGER.info("********starting clean up**********")
        if (carbonLoadModel.isCarbonTransactionalTable) {
          // delete segment is applicable for transactional table
          CarbonLoaderUtil.deleteSegmentForFailure(carbonLoadModel)
          clearIndexFiles(carbonTable, carbonLoadModel.getSegmentId)
        }
        LOGGER.info("********clean up done**********")
        LOGGER.warn("Cannot write load metadata file as data load failed")
        throw new Exception(errorMessage)
      } else {
        // check if data load fails due to bad record and throw data load failure due to
        // bad record exception
        if (loadStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS &&
            status(0)._2._2.failureCauses == FailureCauses.BAD_RECORDS &&
            carbonLoadModel.getBadRecordsAction.split(",")(1) == LoggerAction.FAIL.name) {
          // update the load entry in table status file for changing the status to marked for delete
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uniqueTableStatusId)
          LOGGER.info("********starting clean up**********")
          if (carbonLoadModel.isCarbonTransactionalTable) {
            // delete segment is applicable for transactional table
            CarbonLoaderUtil.deleteSegmentForFailure(carbonLoadModel)
            clearIndexFiles(carbonTable, carbonLoadModel.getSegmentId)
          }
          LOGGER.info("********clean up done**********")
          throw new Exception(status(0)._2._2.errorMsg)
        }
        if (isEmptyDataframe) {
          return null
        }
        // as no record loaded in new segment, new segment should be deleted
        val newEntryLoadStatus =
          if (carbonLoadModel.isCarbonTransactionalTable &&
              !carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.isMV &&
              !CarbonLoaderUtil.isValidSegment(carbonLoadModel,
                carbonLoadModel.getSegmentId.toInt)) {
            LOGGER.warn("Cannot write load metadata file as there is no data to load")
            SegmentStatus.MARKED_FOR_DELETE
          } else {
            loadStatus
          }

        val segmentMetaDataInfo = CommonLoadUtils.getSegmentMetaDataInfoFromAccumulator(
          carbonLoadModel.getSegmentId,
          segmentMetaDataAccumulator)
        segmentMetaDataAccumulator.reset()

        operationContext.setProperty(carbonTable.getTableUniqueName + "_Segment",
          carbonLoadModel.getSegmentId)
        val loadTablePreStatusUpdateEvent: LoadTablePreStatusUpdateEvent =
          new LoadTablePreStatusUpdateEvent(
            carbonTable.getCarbonTableIdentifier,
            carbonLoadModel)
        OperationListenerBus.getInstance()
          .fireEvent(loadTablePreStatusUpdateEvent, operationContext)
        val segmentFileName =
          SegmentFileStore.writeSegmentFile(carbonTable, carbonLoadModel.getSegmentId,
            String.valueOf(carbonLoadModel.getFactTimeStamp), segmentMetaDataInfo)
        val (done, writtenSegment) =
          updateTableStatus(
            sqlContext.sparkSession,
            status,
            carbonLoadModel,
            newEntryLoadStatus,
            overwriteTable,
            segmentFileName,
            updateModel,
            uniqueTableStatusId)
        val loadTablePostStatusUpdateEvent: LoadTablePostStatusUpdateEvent =
          new LoadTablePostStatusUpdateEvent(carbonLoadModel)
        val commitComplete = try {
          OperationListenerBus.getInstance()
            .fireEvent(loadTablePostStatusUpdateEvent, operationContext)
          true
        } catch {
          case ex: Exception =>
            LOGGER.error("Problem while committing indexes", ex)
            false
        }
        if (!done || !commitComplete) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uniqueTableStatusId)
          LOGGER.info("********starting clean up**********")
          if (carbonLoadModel.isCarbonTransactionalTable) {
            // delete segment is applicable for transactional table
            CarbonLoaderUtil.deleteSegmentForFailure(carbonLoadModel)
            // delete corresponding segment file from metadata
            val segmentFile =
              CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath) +
              File.separator + segmentFileName
            FileFactory.deleteFile(segmentFile)
            clearIndexFiles(carbonTable, carbonLoadModel.getSegmentId)
          }
          LOGGER.info("********clean up done**********")
          LOGGER.error("Data load failed due to failure in table status update.")
          throw new Exception("Data load failed due to failure in table status update.")
        }
        if (SegmentStatus.LOAD_PARTIAL_SUCCESS == loadStatus) {
          LOGGER.info("Data load is partially successful for " +
                      s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        } else {
          LOGGER.info("Data load is successful for " +
                      s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        }
        isLoadingCommitted = true
        writtenSegment
      }
    } finally {
      // Release the segment lock, once table status is finally updated
      segmentLock.unlock()
      if (isLoadingCommitted) {
        triggerEventsAfterLoading(sqlContext,
          carbonLoadModel,
          hadoopConf,
          operationContext,
          updateModel.isDefined)
      }
    }
  }

  private def triggerEventsAfterLoading(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration,
      operationContext: OperationContext,
      isUpdateOperation: Boolean): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // code to handle Pre-Priming cache for loading
    if (!StringUtils.isEmpty(carbonLoadModel.getSegmentId)) {
      DistributedRDDUtils.triggerPrepriming(sqlContext.sparkSession, carbonTable, Seq(),
        operationContext, hadoopConf, List(carbonLoadModel.getSegmentId))
    }
    if (isUpdateOperation) {
      // During Update, we cannot perform compaction, as concurrent update and compaction is not
      // allowed. If Update flow, then just return
      return
    }
    try {
      // compaction handling
      if (carbonTable.isHivePartitionTable) {
        carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      }
      val compactedSegments = new util.ArrayList[String]()
      handleSegmentMerging(sqlContext,
        carbonLoadModel
          .getCopyWithPartition(carbonLoadModel.getCsvHeader, carbonLoadModel.getCsvDelimiter),
        carbonTable,
        compactedSegments,
        operationContext)
      carbonLoadModel.setMergedSegmentIds(compactedSegments)
    } catch {
      case e: Exception =>
        LOGGER.error(
          "Auto-Compaction has failed. Ignoring this exception because the load is passed.", e)
    }
  }

  /**
   * clear indexSchema files for segment
   */
  def clearIndexFiles(carbonTable: CarbonTable, segmentId: String): Unit = {
    try {
      val segments = List(new Segment(segmentId)).asJava
      IndexStoreManager.getInstance().getAllCGAndFGIndexes(carbonTable).asScala
        .filter(_.getIndexSchema.isIndex)
        .foreach(_.deleteIndexData(segments))
    } catch {
      case ex : Exception =>
        LOGGER.error(s"Failed to clear indexSchema files for" +
                     s" ${carbonTable.getDatabaseName}.${carbonTable.getTableName}")
    }
  }

  /**
   * Trigger compaction after data load
   */
  def handleSegmentMerging(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      carbonTable: CarbonTable,
      compactedSegments: java.util.List[String],
      operationContext: OperationContext): Unit = {
    LOGGER.info(s"compaction need status is" +
                s" ${ CarbonDataMergerUtil.checkIfAutoLoadMergingRequired(carbonTable) }")
    if (CarbonDataMergerUtil.checkIfAutoLoadMergingRequired(carbonTable)) {
      val compactionSize = 0
      val isCompactionTriggerByDDl = false
      val compactionModel = CompactionModel(
        compactionSize,
        CompactionType.MINOR,
        carbonTable,
        isCompactionTriggerByDDl,
        CarbonFilters.getCurrentPartitions(sqlContext.sparkSession,
          TableIdentifier(carbonTable.getTableName,
            Some(carbonTable.getDatabaseName))), None)

      val isConcurrentCompactionAllowed = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
        CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
      ).equalsIgnoreCase("true")

      if (!isConcurrentCompactionAllowed) {
        handleCompactionForSystemLocking(sqlContext,
          carbonLoadModel,
          CompactionType.MINOR,
          carbonTable,
          compactedSegments,
          compactionModel,
          operationContext
        )
      } else {
        val lock = CarbonLockFactory.getCarbonLockObj(
          carbonTable.getAbsoluteTableIdentifier,
          LockUsage.COMPACTION_LOCK)
        val updateLock = CarbonLockFactory.getCarbonLockObj(carbonTable
          .getAbsoluteTableIdentifier, LockUsage.UPDATE_LOCK)
        try {
          if (updateLock.lockWithRetries()) {
            if (lock.lockWithRetries()) {
              LOGGER.info("Acquired the compaction lock.")
              startCompactionThreads(sqlContext,
                carbonLoadModel,
                compactionModel,
                lock,
                compactedSegments,
                operationContext
              )
            } else {
              LOGGER.error("Not able to acquire the compaction lock for table " +
                           s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName}")
            }
          } else {
            throw new ConcurrentOperationException(carbonTable, "update", "compaction")
          }
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in start compaction thread.", e)
            lock.unlock()
            throw e
        } finally {
          updateLock.unlock()
        }
      }
    }
  }

  /**
   * Update table status file after data loading
   * @param status status collected from each task
   * @param carbonLoadModel load model used for loading
   * @param newEntryLoadStatus segment status to set in the metadata
   * @param overwriteTable true the operation is overwrite
   * @param segmentFileName segment file name
   * @param uuid uuid for the table status file name
   * @return whether operation success and
   *         the segment metadata that written into the segment status file
   */
  private def updateTableStatus(
      session: SparkSession,
      status: Array[(String, (LoadMetadataDetails, ExecutionErrors))],
      carbonLoadModel: CarbonLoadModel,
      newEntryLoadStatus: SegmentStatus,
      overwriteTable: Boolean,
      segmentFileName: String,
      updateModel: Option[UpdateTableModel],
      uuid: String = ""): (Boolean, LoadMetadataDetails) = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val metadataDetails = if (status != null && status.size > 0 && status(0) != null) {
      status(0)._2._1
    } else {
      new LoadMetadataDetails
    }
    metadataDetails.setSegmentFile(segmentFileName)
    CarbonLoaderUtil.populateNewLoadMetaEntry(
        metadataDetails,
        newEntryLoadStatus,
        carbonLoadModel.getFactTimeStamp,
    true)
    CarbonLoaderUtil
      .addDataIndexSizeIntoMetaEntry(metadataDetails, carbonLoadModel.getSegmentId, carbonTable)

    if (!carbonLoadModel.isCarbonTransactionalTable && overwriteTable) {
      CarbonLoaderUtil.deleteNonTransactionalTableForInsertOverwrite(carbonLoadModel)
    }
    var done = true
    // If the updated data should be added as new segment then update the segment information
    if (updateModel.isDefined) {
      done = done && CarbonUpdateUtil.updateTableMetadataStatus(
        carbonLoadModel.getLoadMetadataDetails.asScala.map(l =>
          new Segment(l.getMergedLoadName,
            l.getSegmentFile)).toSet.asJava,
        carbonTable,
        carbonLoadModel.getFactTimeStamp.toString,
        true,
        true,
        updateModel.get.deletedSegments.asJava)
    }
    done = done && CarbonLoaderUtil.recordNewLoadMetadata(metadataDetails, carbonLoadModel, false,
      overwriteTable, uuid, false)
    if (!done) {
      val errorMessage = s"Dataload failed due to failure in table status updation for" +
                         s" ${carbonLoadModel.getTableName}"
      LOGGER.error(errorMessage)
      throw new Exception(errorMessage)
    } else {
      MVManagerInSpark.disableMVOnTable(session, carbonTable, overwriteTable)
    }
    (done, metadataDetails)
  }

  /**
   * Execute load process to load from input DataFrame
   *
   * @param sqlContext sql context
   * @param dataFrame optional DataFrame for insert
   * @param scanResultRDD optional internal row rdd for direct insert
   * @param carbonLoadModel load model
   * @return Return an array that contains all of the elements in NewDataFrameLoaderRDD.
   */
   def loadDataFrame(
      sqlContext: SQLContext,
      dataFrame: Option[DataFrame],
      scanResultRDD: Option[RDD[InternalRow]],
      carbonLoadModel: CarbonLoadModel,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]]
  ): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    try {
      val rdd = if (dataFrame.isDefined) {
        dataFrame.get.rdd
      } else {
        // For internal row, no need of converter and re-arrange step,
        carbonLoadModel.setLoadWithoutConverterWithoutReArrangeStep(true)
        scanResultRDD.get
      }
      val nodeNumOfData = rdd.partitions.flatMap[String, Array[String]] { p =>
        DataLoadPartitionCoalescer.getPreferredLocs(rdd, p).map(_.host)
      }.distinct.length
      val nodes = DistributionUtil.ensureExecutorsByNumberAndGetNodeList(
        nodeNumOfData,
        sqlContext.sparkContext)
      val newRdd =
        if (dataFrame.isDefined) {
          new DataLoadCoalescedRDD[Row](
            sqlContext.sparkSession, dataFrame.get.rdd, nodes.toArray.distinct)
        } else {
          new DataLoadCoalescedRDD[InternalRow](
            sqlContext.sparkSession,
            scanResultRDD.get,
            nodes.toArray.distinct)
        }
      new NewDataFrameLoaderRDD(
        sqlContext.sparkSession,
        new DataLoadResultImpl(),
        carbonLoadModel,
        newRdd,
        segmentMetaDataAccumulator
      ).collect()
    } catch {
      case ex: Exception =>
        LOGGER.error("load data frame failed", ex)
        throw ex
    }
  }

  /**
   * Execute load process to load from input DataFrame
   *
   * @param sqlContext sql context
   * @param dataFrame optional DataFrame for insert
   * @param scanResultRDD optional internal row rdd for direct insert
   * @param carbonLoadModel load model
   * @param segmentMetaDataAccumulator segment metadata accumulator
   * @return Return an array of tuple of uniqueLoadStatusId and tuple of LoadMetadataDetails and
   *         ExecutionErrors
   */
  def loadDataFrameForNoSort(
      sqlContext: SQLContext,
      dataFrame: Option[DataFrame],
      scanResultRDD: Option[RDD[InternalRow]],
      carbonLoadModel: CarbonLoadModel,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]]
  ): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    try {
      val newRdd = if (dataFrame.isDefined) {
        new DataLoadWrapperRDD[Row](sqlContext.sparkSession, dataFrame.get.rdd)
      } else {
        // For internal row, no need of converter and re-arrange step,
        carbonLoadModel.setLoadWithoutConverterWithoutReArrangeStep(true)
        new DataLoadWrapperRDD[InternalRow](sqlContext.sparkSession, scanResultRDD.get)
      }
      new NewDataFrameLoaderRDD(
        sqlContext.sparkSession,
        new DataLoadResultImpl(),
        carbonLoadModel,
        newRdd,
        segmentMetaDataAccumulator
      ).collect()
    } catch {
      case ex: Exception =>
        LOGGER.error("load data frame failed", ex)
        throw ex
    }
  }

  /**
   * Execute load process to load from input file path specified in `carbonLoadModel`
   */
  private def loadDataFile(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration,
      segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]]
  ): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    val nodeBlockMapping = getNodeBlockMapping(sqlContext, hadoopConf, carbonLoadModel)
    val blocksGroupBy: Array[(String, Array[BlockDetails])] = nodeBlockMapping.map { entry =>
      val blockDetailsList =
        entry._2.asScala.map(distributable => {
          val tableBlock = distributable.asInstanceOf[TableBlockInfo]
          new BlockDetails(new Path(tableBlock.getFilePath),
            tableBlock.getBlockOffset, tableBlock.getBlockLength, tableBlock.getLocations
          )
        }).toArray
      (entry._1, blockDetailsList)
    }.toArray

    new NewCarbonDataLoadRDD(
      sqlContext.sparkSession,
      new DataLoadResultImpl(),
      carbonLoadModel,
      blocksGroupBy,
      segmentMetaDataAccumulator
    ).collect()
  }

  def getNodeBlockMapping(
      sqlContext: SQLContext,
      hadoopConf: Configuration,
      carbonLoadModel: CarbonLoadModel): Seq[(String, util.List[Distributable])] = {
    /*
     * when data load handle by node partition
     * 1)clone the hadoop configuration,and set the file path to the configuration
     * 2)use org.apache.hadoop.mapreduce.lib.input.TextInputFormat to get splits,size info
     * 3)use CarbonLoaderUtil.nodeBlockMapping to get mapping info of node and block,
     *   for locally writing carbondata files(one file one block) in nodes
     * 4)use NewCarbonDataLoadRDD to load data and write to carbondata files
     */
    // FileUtils will skip file which is no csv, and return all file path which split by ','
    val filePaths = carbonLoadModel.getFactFilePath
    hadoopConf.set(FileInputFormat.INPUT_DIR, filePaths)
    hadoopConf.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
    hadoopConf.set("io.compression.codecs",
      """org.apache.hadoop.io.compress.GzipCodec,
             org.apache.hadoop.io.compress.DefaultCodec,
             org.apache.hadoop.io.compress.BZip2Codec""".stripMargin)

    CommonUtil.configSplitMaxSize(sqlContext.sparkContext, filePaths, hadoopConf)
    val jobContext = CarbonSparkUtil.createHadoopJob(hadoopConf)
    val inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val blockList = rawSplits.map { inputSplit =>
      val fileSplit = inputSplit.asInstanceOf[FileSplit]
      new TableBlockInfo(fileSplit.getPath.toString,
        fileSplit.getStart, "1",
        fileSplit.getLocations, fileSplit.getLength, ColumnarFormatVersion.V3, null
      ).asInstanceOf[Distributable]
    }
    // group blocks to nodes, tasks
    val startTime = System.currentTimeMillis
    val activeNodes = DistributionUtil
      .ensureExecutorsAndGetNodeList(blockList, sqlContext.sparkContext)
    val skewedDataOptimization = CarbonProperties.getInstance()
      .isLoadSkewedDataOptimizationEnabled
    // get user ddl input the node loads the smallest amount of data
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadMinSize = carbonLoadModel.getLoadMinSize
    if (loadMinSize.equalsIgnoreCase(CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT)) {
      loadMinSize = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
        .getOrElse(CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB,
          CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT)
    }

    val blockAssignStrategy = if (!loadMinSize.equalsIgnoreCase(
      CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT)) {
      CarbonLoaderUtil.BlockAssignmentStrategy.NODE_MIN_SIZE_FIRST
    } else if (skewedDataOptimization) {
      CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_SIZE_FIRST
    } else {
      CarbonLoaderUtil.BlockAssignmentStrategy.BLOCK_NUM_FIRST
    }
    LOGGER.info(s"Allocating block to nodes using strategy: $blockAssignStrategy")

    val nodeBlockMapping = CarbonLoaderUtil.nodeBlockMapping(blockList.toSeq.asJava, -1,
      activeNodes.toList.asJava, blockAssignStrategy, loadMinSize).asScala.toSeq
    val timeElapsed: Long = System.currentTimeMillis - startTime
    LOGGER.info("Total Time taken in block allocation: " + timeElapsed)
    LOGGER.info(s"Total no of blocks: ${ blockList.length }, " +
                s"No.of Nodes: ${ nodeBlockMapping.size }")
    var str = ""
    nodeBlockMapping.foreach { entry =>
      val tableBlock = entry._2
      val totalSize = tableBlock.asScala.map(_.asInstanceOf[TableBlockInfo].getBlockLength).sum
      str = str + "#Node: " + entry._1 + ", no.of.blocks: " + tableBlock.size() +
            f", totalsize.of.blocks: ${ totalSize * 0.1 * 10 / 1024 / 1024 }%.2fMB"
      tableBlock.asScala.foreach(tableBlockInfo =>
        if (!tableBlockInfo.getLocations.exists(hostEntry =>
          hostEntry.equalsIgnoreCase(entry._1)
        )) {
          str = str + " , mismatch locations: " + tableBlockInfo.getLocations
            .foldLeft("")((a, b) => a + "," + b)
        }
      )
      str = str + "\n"
    }
    LOGGER.info(str)
    nodeBlockMapping
  }

}
