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

import com.databricks.spark.csv.newapi.CarbonTextFile

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.sql.{CarbonEnv, DataFrame, SQLContext}
import org.apache.spark.sql.execution.command.{AlterTableModel, CompactionCallableModel, CompactionModel}
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.util.SplitUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonTableIdentifier}
import org.apache.carbondata.core.carbon.datastore.block.{Distributable, TableBlockInfo}
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.integration.spark.merger.{CarbonCompactionUtil, CompactionCallable, CompactionType}
import org.apache.carbondata.lcm.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException
import org.apache.carbondata.spark._
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.merger.CarbonDataMergerUtil
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.{CarbonQueryUtil, LoadMetadataUtil}
import org.apache.spark.{SparkContext, SparkEnv, SparkException}


/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 */
object CarbonDataRDDFactory {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def mergeCarbonData(
      sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      storePath: String) {
    val table = CarbonMetadata.getInstance()
      .getCarbonTable(carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName)
    val metaDataPath: String = table.getMetaDataFilepath
  }

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
    val table = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(databaseName + "_" + tableName)
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
          elem.setModificationOrdeletionTimesStamp(CarbonLoaderUtil.readCurrentTime())
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
            schema,
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

  def alterTableForCompaction(sqlContext: SQLContext,
      alterTableModel: AlterTableModel,
      carbonLoadModel: CarbonLoadModel, storePath: String,
      kettleHomePath: String, storeLocation: String): Unit = {
    var compactionSize: Long = 0
    var compactionType: CompactionType = CompactionType.MINOR_COMPACTION
    if (alterTableModel.compactionType.equalsIgnoreCase("major")) {
      compactionSize = CarbonDataMergerUtil.getCompactionSize(CompactionType.MAJOR_COMPACTION)
      compactionType = CompactionType.MAJOR_COMPACTION
    } else {
      compactionType = CompactionType.MINOR_COMPACTION
    }

    LOGGER.audit(s"Compaction request received for table " +
                 s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val tableCreationTime = CarbonEnv.get.carbonMetastore
      .getTableCreationTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)

    if (null == carbonLoadModel.getLoadMetadataDetails) {
      readLoadMetadataDetails(carbonLoadModel, storePath)
    }
    // reading the start time of data load.
    val loadStartTime = CarbonLoaderUtil.readCurrentTime()
    carbonLoadModel.setFactTimeStamp(loadStartTime)

    val isCompactionTriggerByDDl = true
    val compactionModel = CompactionModel(compactionSize,
      compactionType,
      carbonTable,
      tableCreationTime,
      isCompactionTriggerByDDl
    )

    val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
        CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
      )
      .equalsIgnoreCase("true")

    // if system level compaction is enabled then only one compaction can run in the system
    // if any other request comes at this time then it will create a compaction request file.
    // so that this will be taken up by the compaction process which is executing.
    if (!isConcurrentCompactionAllowed) {
      LOGGER.info("System level compaction lock is enabled.")
      handleCompactionForSystemLocking(sqlContext,
        carbonLoadModel,
        storePath,
        kettleHomePath,
        storeLocation,
        compactionType,
        carbonTable,
        compactionModel
      )
    } else {
      // normal flow of compaction
      val lock = CarbonLockFactory
        .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
          LockUsage.COMPACTION_LOCK
        )

      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the compaction lock for table" +
                    s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        try {
          startCompactionThreads(sqlContext,
            carbonLoadModel,
            storePath,
            kettleHomePath,
            storeLocation,
            compactionModel,
            lock
          )
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
            lock.unlock()
        }
      } else {
        LOGGER.audit("Not able to acquire the compaction lock for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.error(s"Not able to acquire the compaction lock for table" +
                     s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        sys.error("Table is already locked for compaction. Please try after some time.")
      }
    }
  }

  def handleCompactionForSystemLocking(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      kettleHomePath: String,
      storeLocation: String,
      compactionType: CompactionType,
      carbonTable: CarbonTable,
      compactionModel: CompactionModel): Unit = {
    val lock = CarbonLockFactory
      .getCarbonLockObj(CarbonCommonConstants.SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER,
        LockUsage.SYSTEMLEVEL_COMPACTION_LOCK
      )
    if (lock.lockWithRetries()) {
      LOGGER.info(s"Acquired the compaction lock for table ${ carbonLoadModel.getDatabaseName }" +
                  s".${ carbonLoadModel.getTableName }")
      try {
        startCompactionThreads(sqlContext,
          carbonLoadModel,
          storePath,
          kettleHomePath,
          storeLocation,
          compactionModel,
          lock
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
      LOGGER.audit("Not able to acquire the system level compaction lock for table " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      LOGGER.error("Not able to acquire the compaction lock for table " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      CarbonCompactionUtil
        .createCompactionRequiredFile(carbonTable.getMetaDataFilepath, compactionType)
      // do sys error only in case of DDL trigger.
      if (compactionModel.isDDLTrigger) {
        sys.error("Compaction is in progress, compaction request for table " +
                  s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }" +
                  " is in queue.")
      } else {
        LOGGER.error("Compaction is in progress, compaction request for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }" +
                     " is in queue.")
      }
    }
  }

  def executeCompaction(carbonLoadModel: CarbonLoadModel,
      storePath: String,
      compactionModel: CompactionModel,
      executor: ExecutorService,
      sqlContext: SQLContext,
      kettleHomePath: String,
      storeLocation: String): Unit = {
    val sortedSegments: util.List[LoadMetadataDetails] = new util.ArrayList[LoadMetadataDetails](
      carbonLoadModel.getLoadMetadataDetails
    )
    CarbonDataMergerUtil.sortSegments(sortedSegments)

    var segList = carbonLoadModel.getLoadMetadataDetails
    var loadsToMerge = CarbonDataMergerUtil.identifySegmentsToBeMerged(
      storePath,
      carbonLoadModel,
      compactionModel.compactionSize,
      segList,
      compactionModel.compactionType
    )
    while (loadsToMerge.size() > 1) {
      val lastSegment = sortedSegments.get(sortedSegments.size() - 1)
      deletePartialLoadsInCompaction(carbonLoadModel)
      val futureList: util.List[Future[Void]] = new util.ArrayList[Future[Void]](
        CarbonCommonConstants
          .DEFAULT_COLLECTION_SIZE
      )

      scanSegmentsAndSubmitJob(futureList,
        loadsToMerge,
        executor,
        storePath,
        sqlContext,
        compactionModel,
        kettleHomePath,
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
          LOGGER.error(s"Exception in compaction thread ${ e.getMessage }")
          throw e
      }


      // scan again and determine if anything is there to merge again.
      readLoadMetadataDetails(carbonLoadModel, storePath)
      segList = carbonLoadModel.getLoadMetadataDetails
      // in case of major compaction we will scan only once and come out as it will keep
      // on doing major for the new loads also.
      // excluding the newly added segments.
      if (compactionModel.compactionType == CompactionType.MAJOR_COMPACTION) {

        segList = CarbonDataMergerUtil
          .filterOutNewlyAddedSegments(carbonLoadModel.getLoadMetadataDetails, lastSegment)
      }
      loadsToMerge = CarbonDataMergerUtil.identifySegmentsToBeMerged(
        storePath,
        carbonLoadModel,
        compactionModel.compactionSize,
        segList,
        compactionModel.compactionType
      )
    }
  }

  /**
   * This will submit the loads to be merged into the executor.
   *
   * @param futureList
   */
  def scanSegmentsAndSubmitJob(futureList: util.List[Future[Void]],
      loadsToMerge: util
      .List[LoadMetadataDetails],
      executor: ExecutorService,
      storePath: String,
      sqlContext: SQLContext,
      compactionModel: CompactionModel,
      kettleHomePath: String,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String): Unit = {

    loadsToMerge.asScala.foreach(seg => {
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }
    )

    val compactionCallableModel = CompactionCallableModel(storePath,
      carbonLoadModel,
      storeLocation,
      compactionModel.carbonTable,
      kettleHomePath,
      compactionModel.tableCreationTime,
      loadsToMerge,
      sqlContext,
      compactionModel.compactionType
    )

    val future: Future[Void] = executor
      .submit(new CompactionCallable(compactionCallableModel
      )
      )
    futureList.add(future)
  }

  def startCompactionThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      kettleHomePath: String,
      storeLocation: String,
      compactionModel: CompactionModel,
      compactionLock: ICarbonLock): Unit = {
    val executor: ExecutorService = Executors.newFixedThreadPool(1)
    // update the updated table status.
    readLoadMetadataDetails(carbonLoadModel, storePath)
    var segList: util.List[LoadMetadataDetails] = carbonLoadModel.getLoadMetadataDetails

    // clean up of the stale segments.
    try {
      CarbonLoaderUtil.deletePartialLoadDataIfExist(carbonLoadModel, true)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in compaction thread while clean up of stale segments" +
                     s" ${ e.getMessage }")
    }

    val compactionThread = new Thread {
      override def run(): Unit = {

        try {
          // compaction status of the table which is triggered by the user.
          var triggeredCompactionStatus = false
          var exception: Exception = null
          try {
            executeCompaction(carbonLoadModel: CarbonLoadModel,
              storePath: String,
              compactionModel: CompactionModel,
              executor, sqlContext, kettleHomePath, storeLocation
            )
            triggeredCompactionStatus = true
          } catch {
            case e: Exception =>
              LOGGER.error(s"Exception in compaction thread ${ e.getMessage }")
              exception = e
          }
          // continue in case of exception also, check for all the tables.
          val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
              CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
            ).equalsIgnoreCase("true")

          if (!isConcurrentCompactionAllowed) {
            LOGGER.info("System level compaction lock is enabled.")
            val skipCompactionTables = ListBuffer[CarbonTableIdentifier]()
            var tableForCompaction = CarbonCompactionUtil.getNextTableToCompact(
              CarbonEnv.get.carbonMetastore.metadata.tablesMeta.toArray,
              skipCompactionTables.toList.asJava)
            while (null != tableForCompaction) {
              LOGGER.info("Compaction request has been identified for table " +
                          s"${ tableForCompaction.carbonTable.getDatabaseName }." +
                          s"${ tableForCompaction.carbonTableIdentifier.getTableName }")
              val table: CarbonTable = tableForCompaction.carbonTable
              val metadataPath = table.getMetaDataFilepath
              val compactionType = CarbonCompactionUtil.determineCompactionType(metadataPath)

              val newCarbonLoadModel = new CarbonLoadModel()
              prepareCarbonLoadModel(storePath, table, newCarbonLoadModel)
              val tableCreationTime = CarbonEnv.get.carbonMetastore
                .getTableCreationTime(newCarbonLoadModel.getDatabaseName,
                  newCarbonLoadModel.getTableName
                )

              val compactionSize = CarbonDataMergerUtil
                .getCompactionSize(CompactionType.MAJOR_COMPACTION)

              val newcompactionModel = CompactionModel(compactionSize,
                compactionType,
                table,
                tableCreationTime,
                compactionModel.isDDLTrigger
              )
              // proceed for compaction
              try {
                executeCompaction(newCarbonLoadModel,
                  newCarbonLoadModel.getStorePath,
                  newcompactionModel,
                  executor, sqlContext, kettleHomePath, storeLocation
                )
              } catch {
                case e: Exception =>
                  LOGGER.error("Exception in compaction thread for table " +
                               s"${ tableForCompaction.carbonTable.getDatabaseName }." +
                               s"${ tableForCompaction.carbonTableIdentifier.getTableName }")
                // not handling the exception. only logging as this is not the table triggered
                // by user.
              } finally {
                // delete the compaction required file in case of failure or success also.
                if (!CarbonCompactionUtil
                  .deleteCompactionRequiredFile(metadataPath, compactionType)) {
                  // if the compaction request file is not been able to delete then
                  // add those tables details to the skip list so that it wont be considered next.
                  skipCompactionTables.+=:(tableForCompaction.carbonTableIdentifier)
                  LOGGER.error("Compaction request file can not be deleted for table " +
                               s"${ tableForCompaction.carbonTable.getDatabaseName }." +
                               s"${ tableForCompaction.carbonTableIdentifier.getTableName }")
                }
              }
              // ********* check again for all the tables.
              tableForCompaction = CarbonCompactionUtil
                .getNextTableToCompact(CarbonEnv.get.carbonMetastore.metadata
                  .tablesMeta.toArray, skipCompactionTables.asJava
                )
            }
            // giving the user his error for telling in the beeline if his triggered table
            // compaction is failed.
            if (!triggeredCompactionStatus) {
              throw new Exception("Exception in compaction " + exception.getMessage)
            }
          }
        } finally {
          executor.shutdownNow()
          deletePartialLoadsInCompaction(carbonLoadModel)
          compactionLock.unlock()
        }
      }
    }
    // calling the run method of a thread to make the call as blocking call.
    // in the future we may make this as concurrent.
    compactionThread.run()
  }

  def prepareCarbonLoadModel(storePath: String,
      table: CarbonTable,
      newCarbonLoadModel: CarbonLoadModel): Unit = {
    newCarbonLoadModel.setAggTables(table.getAggregateTablesName.asScala.toArray)
    newCarbonLoadModel.setTableName(table.getFactTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    newCarbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    newCarbonLoadModel.setTableName(table.getCarbonTableIdentifier.getTableName)
    newCarbonLoadModel.setDatabaseName(table.getCarbonTableIdentifier.getDatabaseName)
    newCarbonLoadModel.setStorePath(table.getStorePath)
    readLoadMetadataDetails(newCarbonLoadModel, storePath)
    val loadStartTime = CarbonLoaderUtil.readCurrentTime()
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

  def loadCarbonData(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      kettleHomePath: String,
      columinar: Boolean,
      partitionStatus: String = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS,
      useKettle: Boolean,
      dataFrame: Option[DataFrame] = None): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val isAgg = false
    // for handling of the segment Merging.
    def handleSegmentMerging(tableCreationTime: Long): Unit = {
      LOGGER.info(s"compaction need status is" +
                  s" ${ CarbonDataMergerUtil.checkIfAutoLoadMergingRequired() }")
      if (CarbonDataMergerUtil.checkIfAutoLoadMergingRequired()) {
        LOGGER.audit(s"Compaction request received for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        val compactionSize = 0
        val isCompactionTriggerByDDl = false
        val compactionModel = CompactionModel(compactionSize,
          CompactionType.MINOR_COMPACTION,
          carbonTable,
          tableCreationTime,
          isCompactionTriggerByDDl
        )
        var storeLocation = ""
        val configuredStore = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != configuredStore && configuredStore.nonEmpty) {
          storeLocation = configuredStore(Random.nextInt(configuredStore.length))
        }
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
        storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()

        val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
            CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
          )
          .equalsIgnoreCase("true")

        if (!isConcurrentCompactionAllowed) {

          handleCompactionForSystemLocking(sqlContext,
            carbonLoadModel,
            storePath,
            kettleHomePath,
            storeLocation,
            CompactionType.MINOR_COMPACTION,
            carbonTable,
            compactionModel
          )
        } else {
          val lock = CarbonLockFactory
            .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
              LockUsage.COMPACTION_LOCK
            )

          if (lock.lockWithRetries()) {
            LOGGER.info("Acquired the compaction lock.")
            try {
              startCompactionThreads(sqlContext,
                carbonLoadModel,
                storePath,
                kettleHomePath,
                storeLocation,
                compactionModel,
                lock
              )
            } catch {
              case e: Exception =>
                LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
                lock.unlock()
                throw e
            }
          } else {
            LOGGER.audit("Not able to acquire the compaction lock for table " +
                         s"${ carbonLoadModel.getDatabaseName }.${
                           carbonLoadModel
                             .getTableName
                         }")
            LOGGER.error("Not able to acquire the compaction lock for table " +
                         s"${ carbonLoadModel.getDatabaseName }.${
                           carbonLoadModel
                             .getTableName
                         }")
          }
        }
      }
    }

    try {
      LOGGER.audit(s"Data load request has been received for table" +
                   s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      if (!useKettle) {
        LOGGER.audit("Data is loading with New Data Flow for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      }
      // Check if any load need to be deleted before loading new data
      deleteLoadsAndUpdateMetadata(carbonLoadModel, carbonTable, storePath,
        isForceDeletion = false)
      if (null == carbonLoadModel.getLoadMetadataDetails) {
        readLoadMetadataDetails(carbonLoadModel, storePath)
      }

      var currentLoadCount = -1
      val convLoadDetails = carbonLoadModel.getLoadMetadataDetails.asScala
      // taking the latest segment ID present.
      // so that any other segments above this will be deleted.
      if (convLoadDetails.nonEmpty) {
        convLoadDetails.foreach { l =>
          var loadCount = 0
          breakable {
            try {
              loadCount = Integer.parseInt(l.getLoadName)
            } catch {
              case e: NumberFormatException => // case of merge folder. ignore it.
                break
            }
            if (currentLoadCount < loadCount) {
              currentLoadCount = loadCount
            }
          }
        }
      }
      currentLoadCount += 1
      // Deleting the any partially loaded data if present.
      // in some case the segment folder which is present in store will not have entry in status.
      // so deleting those folders.
      try {
        CarbonLoaderUtil.deletePartialLoadDataIfExist(carbonLoadModel, false)
      } catch {
        case e: Exception =>
          LOGGER
            .error(s"Exception in data load while clean up of stale segments ${ e.getMessage }")
      }

      // reading the start time of data load.
      val loadStartTime = CarbonLoaderUtil.readCurrentTime()
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      val tableCreationTime = CarbonEnv.get.carbonMetastore
        .getTableCreationTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
      val schemaLastUpdatedTime = CarbonEnv.get.carbonMetastore
        .getSchemaLastUpdatedTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)

      // get partition way from configuration
      // val isTableSplitPartition = CarbonProperties.getInstance().getProperty(
      // CarbonCommonConstants.TABLE_SPLIT_PARTITION,
      // CarbonCommonConstants.TABLE_SPLIT_PARTITION_DEFAULT_VALUE).toBoolean
      val isTableSplitPartition = false
      var blocksGroupBy: Array[(String, Array[BlockDetails])] = null
      var status: Array[(String, LoadMetadataDetails)] = null

      def loadDataFile(): Unit = {
        if (isTableSplitPartition) {
          /*
         * when data handle by table split partition
         * 1) get partition files, direct load or not will get the different files path
         * 2) get files blocks by using SplitUtils
         * 3) output Array[(partitionID,Array[BlockDetails])] to blocksGroupBy
         */
          var splits = Array[TableSplit]()
          if (carbonLoadModel.isDirectLoad) {
            // get all table Splits, this part means files were divide to different partitions
            splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath)
            // get all partition blocks from file list
            blocksGroupBy = splits.map {
              split =>
                val pathBuilder = new StringBuilder()
                for (path <- split.getPartition.getFilesPath.asScala) {
                  pathBuilder.append(path).append(",")
                }
                if (pathBuilder.nonEmpty) {
                  pathBuilder.substring(0, pathBuilder.size - 1)
                }
                (split.getPartition.getUniqueID, SplitUtils.getSplits(pathBuilder.toString(),
                  sqlContext.sparkContext
                ))
            }
          } else {
            // get all table Splits,when come to this, means data have been partition
            splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
              carbonLoadModel.getTableName, null
            )
            // get all partition blocks from factFilePath/uniqueID/
            blocksGroupBy = splits.map {
              split =>
                val pathBuilder = new StringBuilder()
                pathBuilder.append(carbonLoadModel.getFactFilePath)
                if (!carbonLoadModel.getFactFilePath.endsWith("/")
                    && !carbonLoadModel.getFactFilePath.endsWith("\\")) {
                  pathBuilder.append("/")
                }
                pathBuilder.append(split.getPartition.getUniqueID).append("/")
                (split.getPartition.getUniqueID,
                  SplitUtils.getSplits(pathBuilder.toString, sqlContext.sparkContext))
            }
          }
        } else {
          /*
         * when data load handle by node partition
         * 1)clone the hadoop configuration,and set the file path to the configuration
         * 2)use NewHadoopRDD to get split,size:Math.max(minSize, Math.min(maxSize, blockSize))
         * 3)use DummyLoadRDD to group blocks by host,and let spark balance the block location
         * 4)DummyLoadRDD output (host,Array[BlockDetails])as the parameter to CarbonDataLoadRDD
         *   which parititon by host
         */
          val hadoopConfiguration = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
          // FileUtils will skip file which is no csv, and return all file path which split by ','
          val filePaths = carbonLoadModel.getFactFilePath
          hadoopConfiguration.set(FileInputFormat.INPUT_DIR, filePaths)
          hadoopConfiguration.set(FileInputFormat.INPUT_DIR_RECURSIVE, "true")
          hadoopConfiguration.set("io.compression.codecs",
            """org.apache.hadoop.io.compress.GzipCodec,
               org.apache.hadoop.io.compress.DefaultCodec,
               org.apache.hadoop.io.compress.BZip2Codec""".stripMargin)

          CarbonTextFile.configSplitMaxSize(sqlContext.sparkContext, filePaths, hadoopConfiguration)

          val inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat
          inputFormat match {
            case configurable: Configurable =>
              configurable.setConf(hadoopConfiguration)
            case _ =>
          }
          val jobContext = new Job(hadoopConfiguration)
          val rawSplits = inputFormat.getSplits(jobContext).toArray
          val blockList = rawSplits.map(inputSplit => {
            val fileSplit = inputSplit.asInstanceOf[FileSplit]
            new TableBlockInfo(fileSplit.getPath.toString,
              fileSplit.getStart, "1",
              fileSplit.getLocations, fileSplit.getLength
            ).asInstanceOf[Distributable]
          }
          )
          // group blocks to nodes, tasks
          val startTime = System.currentTimeMillis
          val activeNodes = DistributionUtil
            .ensureExecutorsAndGetNodeList(blockList, sqlContext.sparkContext)
          val nodeBlockMapping =
            CarbonLoaderUtil
              .nodeBlockMapping(blockList.toSeq.asJava, -1, activeNodes.toList.asJava).asScala
              .toSeq
          val timeElapsed: Long = System.currentTimeMillis - startTime
          LOGGER.info("Total Time taken in block allocation: " + timeElapsed)
          LOGGER.info(s"Total no of blocks: ${ blockList.length }, " +
                      s"No.of Nodes: ${nodeBlockMapping.size}")
          var str = ""
          nodeBlockMapping.foreach(entry => {
            val tableBlock = entry._2
            str = str + "#Node: " + entry._1 + " no.of.blocks: " + tableBlock.size()
            tableBlock.asScala.foreach(tableBlockInfo =>
              if (!tableBlockInfo.getLocations.exists(hostentry =>
                hostentry.equalsIgnoreCase(entry._1)
              )) {
                str = str + " , mismatch locations: " + tableBlockInfo.getLocations
                  .foldLeft("")((a, b) => a + "," + b)
              }
            )
            str = str + "\n"
          }
          )
          LOGGER.info(str)
          blocksGroupBy = nodeBlockMapping.map(entry => {
            val blockDetailsList =
              entry._2.asScala.map(distributable => {
                val tableBlock = distributable.asInstanceOf[TableBlockInfo]
                new BlockDetails(new Path(tableBlock.getFilePath),
                  tableBlock.getBlockOffset, tableBlock.getBlockLength, tableBlock.getLocations
                )
              }).toArray
            (entry._1, blockDetailsList)
          }
          ).toArray
        }

        if (useKettle) {
          status = new DataFileLoaderRDD(sqlContext.sparkContext,
            new DataLoadResultImpl(),
            carbonLoadModel,
            storePath,
            kettleHomePath,
            columinar,
            currentLoadCount,
            tableCreationTime,
            schemaLastUpdatedTime,
            blocksGroupBy,
            isTableSplitPartition
          ).collect()
        } else {
          status = new NewCarbonDataLoadRDD(sqlContext.sparkContext,
            new DataLoadResultImpl(),
            carbonLoadModel,
            currentLoadCount,
            blocksGroupBy,
            isTableSplitPartition).collect()
        }
      }

      def loadDataFrame(): Unit = {
        var rdd = dataFrame.get.rdd
        var numPartitions = DistributionUtil.getNodeList(sqlContext.sparkContext).length
        numPartitions = Math.max(1, Math.min(numPartitions, rdd.partitions.length))
        rdd = rdd.coalesce(numPartitions, shuffle = false)

        status = new DataFrameLoaderRDD(sqlContext.sparkContext,
          new DataLoadResultImpl(),
          carbonLoadModel,
          storePath,
          kettleHomePath,
          columinar,
          currentLoadCount,
          tableCreationTime,
          schemaLastUpdatedTime,
          rdd).collect()
      }

      CarbonLoaderUtil.checkAndCreateCarbonDataLocation(storePath,
        carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName, currentLoadCount.toString)
      var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      var errorMessage: String = "DataLoad failure"
      var executorMessage: String = ""
      try {
        if (dataFrame.isDefined) {
          loadDataFrame()
        } else {
          loadDataFile()
        }
        val newStatusMap = scala.collection.mutable.Map.empty[String, String]
        status.foreach { eachLoadStatus =>
          val state = newStatusMap.get(eachLoadStatus._1)
          state match {
            case Some(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) =>
              newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2.getLoadStatus)
            case Some(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
              if eachLoadStatus._2.getLoadStatus ==
                 CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS =>
              newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2.getLoadStatus)
            case _ =>
              newStatusMap.put(eachLoadStatus._1, eachLoadStatus._2.getLoadStatus)
          }
        }

        newStatusMap.foreach {
          case (key, value) =>
            if (value == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
              loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
            } else if (value == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
                       !loadStatus.equals(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
              loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
            }
        }

        if (loadStatus != CarbonCommonConstants.STORE_LOADSTATUS_FAILURE &&
            partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
          loadStatus = partitionStatus
        }
      } catch {
        case ex: Throwable =>
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          ex match {
            case sparkException: SparkException =>
              if (sparkException.getCause.isInstanceOf[DataLoadingException] ||
                  sparkException.getCause.isInstanceOf[CarbonDataLoadingException]) {
                executorMessage = sparkException.getCause.getMessage
                errorMessage = errorMessage + ": " + executorMessage
              }
            case _ =>
              executorMessage = ex.getCause.getMessage
              errorMessage = errorMessage + ": " + executorMessage
          }
          LOGGER.info(errorMessage)
          LOGGER.error(ex)
      }

      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
        LOGGER.info("********starting clean up**********")
        CarbonLoaderUtil.deleteSegment(carbonLoadModel, currentLoadCount)
        LOGGER.info("********clean up done**********")
        LOGGER.audit(s"Data load is failed for " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.warn("Cannot write load metadata file as data load failed")
        throw new Exception(errorMessage)
      } else {
        val metadataDetails = status(0)._2
        if (!isAgg) {
          val status = CarbonLoaderUtil.recordLoadMetadata(currentLoadCount, metadataDetails,
            carbonLoadModel, loadStatus, loadStartTime)
          if (!status) {
            val errorMessage = "Dataload failed due to failure in table status updation."
            LOGGER.audit("Data load is failed for " +
                         s"${ carbonLoadModel.getDatabaseName }.${
                           carbonLoadModel
                             .getTableName
                         }")
            LOGGER.error("Dataload failed due to failure in table status updation.")
            throw new Exception(errorMessage)
          }
        } else if (!carbonLoadModel.isRetentionRequest) {
          // TODO : Handle it
          LOGGER.info("********Database updated**********")
        }
        LOGGER.audit("Data load is successful for " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        try {
          // compaction handling
          handleSegmentMerging(tableCreationTime)
        } catch {
          case e: Exception =>
            throw new Exception(
              "Dataload is success. Auto-Compaction has failed. Please check logs.")
        }
      }
    }

  }

  def readLoadMetadataDetails(model: CarbonLoadModel, storePath: String): Unit = {
    val metadataPath = model.getCarbonDataLoadSchema.getCarbonTable.getMetaDataFilepath
    val details = SegmentStatusManager.readLoadMetadata(metadataPath)
    model.setLoadMetadataDetails(details.toList.asJava)
  }

  def deleteLoadsAndUpdateMetadata(
      carbonLoadModel: CarbonLoadModel,
      table: CarbonTable,
      storePath: String,
      isForceDeletion: Boolean): Unit = {
    if (LoadMetadataUtil.isLoadDeletionRequired(carbonLoadModel)) {
      val loadMetadataFilePath = CarbonLoaderUtil
        .extractLoadMetadataFileLocation(carbonLoadModel)
      val details = SegmentStatusManager.readLoadMetadata(loadMetadataFilePath)
      val carbonTableStatusLock = CarbonLockFactory
        .getCarbonLockObj(table.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
          LockUsage.TABLE_STATUS_LOCK)

      // Delete marked loads
      val isUpdationRequired = DeleteLoadFolders
        .deleteLoadFoldersFromFileSystem(carbonLoadModel, storePath, isForceDeletion, details)

      if (isUpdationRequired) {
        try {
          // Update load metadate file after cleaning deleted nodes
          if (carbonTableStatusLock.lockWithRetries()) {
            LOGGER.info("Table status lock has been successfully acquired.")

            // read latest table status again.
            val latestMetadata = SegmentStatusManager.readLoadMetadata(loadMetadataFilePath)

            // update the metadata details from old to new status.
            val latestStatus = CarbonLoaderUtil
              .updateLoadMetadataFromOldToNew(details, latestMetadata)

            CarbonLoaderUtil.writeLoadMetadata(
              carbonLoadModel.getCarbonDataLoadSchema,
              carbonLoadModel.getDatabaseName,
              carbonLoadModel.getTableName, latestStatus)
          } else {
            val errorMsg = "Clean files request is failed for " +
              s"${ carbonLoadModel.getDatabaseName }." +
              s"${ carbonLoadModel.getTableName }" +
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

  def dropTable(
      sc: SparkContext,
      schema: String,
      table: String) {
    val v: Value[Array[Object]] = new ValueImpl()
    new CarbonDropTableRDD(sc, v, schema, table).collect
  }

  def cleanFiles(
      sc: SparkContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String) {
    val table = org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName)
    val metaDataPath: String = table.getMetaDataFilepath
    val carbonCleanFilesLock = CarbonLockFactory
      .getCarbonLockObj(table.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.CLEAN_FILES_LOCK
      )
    try {
      if (carbonCleanFilesLock.lockWithRetries()) {
        LOGGER.info("Clean files lock has been successfully acquired.")
        deleteLoadsAndUpdateMetadata(carbonLoadModel,
          table,
          storePath,
          isForceDeletion = true)
      } else {
        val errorMsg = "Clean files request is failed for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }" +
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
