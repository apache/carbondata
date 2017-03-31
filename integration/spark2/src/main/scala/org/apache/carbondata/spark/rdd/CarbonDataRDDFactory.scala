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
import java.util.UUID
import java.util.concurrent._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionCoalescer, UpdateCoalescedRDD}
import org.apache.spark.sql.{CarbonEnv, DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.command.{AlterTableModel, CompactionModel, ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.{Distributable, TableBlockInfo}
import org.apache.carbondata.core.dictionary.server.DictionaryServer
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.csvload.BlockDetails
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException
import org.apache.carbondata.spark._
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.merger.{CarbonCompactionUtil, CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.{CarbonQueryUtil, CommonUtil}

/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 */
object CarbonDataRDDFactory {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def alterTableForCompaction(sqlContext: SQLContext,
      alterTableModel: AlterTableModel,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      storeLocation: String): Unit = {
    var compactionSize: Long = 0
    var compactionType: CompactionType = CompactionType.MINOR_COMPACTION
    if (alterTableModel.compactionType.equalsIgnoreCase("major")) {
      compactionSize = CarbonDataMergerUtil.getCompactionSize(CompactionType.MAJOR_COMPACTION)
      compactionType = CompactionType.MAJOR_COMPACTION
    } else if (alterTableModel.compactionType.equalsIgnoreCase("IUD_UPDDEL_DELTA_COMPACTION")) {
      compactionType = CompactionType.IUD_UPDDEL_DELTA_COMPACTION
      if (alterTableModel.segmentUpdateStatusManager.get != None) {
        carbonLoadModel
          .setSegmentUpdateStatusManager(alterTableModel.segmentUpdateStatusManager.get)

        carbonLoadModel
          .setSegmentUpdateDetails(alterTableModel.segmentUpdateStatusManager.get
            .getUpdateStatusDetails.toList.asJava)

        carbonLoadModel
          .setLoadMetadataDetails(alterTableModel.segmentUpdateStatusManager.get
            .getLoadMetadataDetails.toList.asJava)
      }
    }
    else {
      compactionType = CompactionType.MINOR_COMPACTION
    }

    LOGGER.audit(s"Compaction request received for table " +
        s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val tableCreationTime = CarbonEnv.get.carbonMetastore
        .getTableCreationTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)

    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel, storePath)
    }
    // reading the start time of data load.
    val loadStartTime : Long =
    if (alterTableModel.factTimeStamp.isEmpty) {
      CarbonUpdateUtil.readCurrentTime
    } else {
      alterTableModel.factTimeStamp.get
    }
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

  def startCompactionThreads(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      storeLocation: String,
      compactionModel: CompactionModel,
      compactionLock: ICarbonLock): Unit = {
    val executor: ExecutorService = Executors.newFixedThreadPool(1)
    // update the updated table status.
    if (compactionModel.compactionType != CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
      // update the updated table status. For the case of Update Delta Compaction the Metadata
      // is filled in LoadModel, no need to refresh.
      CommonUtil.readLoadMetadataDetails(carbonLoadModel, storePath)
    }

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
            DataManagementFunc.executeCompaction(carbonLoadModel: CarbonLoadModel,
              storePath: String,
              compactionModel: CompactionModel,
              executor, sqlContext, storeLocation
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
            var tableForCompaction = CarbonCompactionUtil
                .getNextTableToCompact(CarbonEnv.get.carbonMetastore.metadata.tablesMeta.toArray,
                  skipCompactionTables.toList.asJava)
            while (null != tableForCompaction) {
              LOGGER.info("Compaction request has been identified for table " +
                  s"${ tableForCompaction.carbonTable.getDatabaseName }." +
                  s"${ tableForCompaction.carbonTableIdentifier.getTableName }")
              val table: CarbonTable = tableForCompaction.carbonTable
              val metadataPath = table.getMetaDataFilepath
              val compactionType = CarbonCompactionUtil.determineCompactionType(metadataPath)

              val newCarbonLoadModel = new CarbonLoadModel()
              DataManagementFunc.prepareCarbonLoadModel(storePath, table, newCarbonLoadModel)
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
                DataManagementFunc.executeCompaction(newCarbonLoadModel,
                  newCarbonLoadModel.getStorePath,
                  newcompactionModel,
                  executor, sqlContext, storeLocation
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
          DataManagementFunc.deletePartialLoadsInCompaction(carbonLoadModel)
          compactionLock.unlock()
        }
      }
    }
    // calling the run method of a thread to make the call as blocking call.
    // in the future we may make this as concurrent.
    compactionThread.run()
  }

  def loadCarbonData(sqlContext: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storePath: String,
      columnar: Boolean,
      partitionStatus: String = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS,
      result: Future[DictionaryServer],
      dataFrame: Option[DataFrame] = None,
      updateModel: Option[UpdateTableModel] = None): Unit = {
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
      // Check if any load need to be deleted before loading new data
      DataManagementFunc.deleteLoadsAndUpdateMetadata(carbonLoadModel.getDatabaseName,
        carbonLoadModel.getTableName, storePath, isForceDeletion = false)
      if (null == carbonLoadModel.getLoadMetadataDetails) {
        CommonUtil.readLoadMetadataDetails(carbonLoadModel, storePath)
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
      val loadStartTime = CarbonUpdateUtil.readCurrentTime();
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
      var res: Array[List[(String, (LoadMetadataDetails, ExecutionErrors))]] = null

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
                (split.getPartition.getUniqueID, SparkUtil.getSplits(pathBuilder.toString(),
                  sqlContext.sparkContext
                ))
            }
          } else {
            // get all table Splits,when come to this, means data have been partition
            splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
              carbonLoadModel.getTableName, null)
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
                    SparkUtil.getSplits(pathBuilder.toString, sqlContext.sparkContext))
            }
          }
        } else {
          /*
           * when data load handle by node partition
           * 1)clone the hadoop configuration,and set the file path to the configuration
           * 2)use org.apache.hadoop.mapreduce.lib.input.TextInputFormat to get splits,size info
           * 3)use CarbonLoaderUtil.nodeBlockMapping to get mapping info of node and block,
           *   for locally writing carbondata files(one file one block) in nodes
           * 4)use NewCarbonDataLoadRDD to load data and write to carbondata files
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

          CommonUtil.configSplitMaxSize(sqlContext.sparkContext, filePaths, hadoopConfiguration)

          val inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat
          val jobContext = new Job(hadoopConfiguration)
          val rawSplits = inputFormat.getSplits(jobContext).toArray
          val blockList = rawSplits.map { inputSplit =>
            val fileSplit = inputSplit.asInstanceOf[FileSplit]
            new TableBlockInfo(fileSplit.getPath.toString,
              fileSplit.getStart, "1",
              fileSplit.getLocations, fileSplit.getLength, ColumnarFormatVersion.V1
            ).asInstanceOf[Distributable]
          }
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

        status = new NewCarbonDataLoadRDD(sqlContext.sparkContext,
          new DataLoadResultImpl(),
          carbonLoadModel,
          currentLoadCount,
          blocksGroupBy,
          isTableSplitPartition).collect()
      }

      def loadDataFrame(): Unit = {
        try {
          val rdd = dataFrame.get.rdd

          val nodeNumOfData = rdd.partitions.flatMap[String, Array[String]]{ p =>
            DataLoadPartitionCoalescer.getPreferredLocs(rdd, p).map(_.host)
          }.distinct.size
          val nodes = DistributionUtil.ensureExecutorsByNumberAndGetNodeList(nodeNumOfData,
            sqlContext.sparkContext)
          val newRdd = new DataLoadCoalescedRDD[Row](rdd, nodes.toArray.distinct)

          status = new NewDataFrameLoaderRDD(sqlContext.sparkContext,
            new DataLoadResultImpl(),
            carbonLoadModel,
            currentLoadCount,
            tableCreationTime,
            schemaLastUpdatedTime,
            newRdd).collect()

        } catch {
          case ex: Exception =>
            LOGGER.error(ex, "load data frame failed")
            throw ex
        }
      }

      if (!updateModel.isDefined) {
      CarbonLoaderUtil.checkAndCreateCarbonDataLocation(storePath,
        carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName, currentLoadCount.toString)
      }
      var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      var errorMessage: String = "DataLoad failure"
      var executorMessage: String = ""
      try {
        if (dataFrame.isDefined) {
          loadDataFrame()
        }
        else {
          loadDataFile()
        }
        if (updateModel.isDefined) {

          res.foreach(resultOfSeg => resultOfSeg.foreach(
            resultOfBlock => {
              if (resultOfBlock._2._1.getLoadStatus
                .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
                loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
                if (resultOfBlock._2._2.failureCauses == FailureCauses.NONE) {
                  updateModel.get.executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
                  updateModel.get.executorErrors.errorMsg = "Failure in the Executor."
                }
                else {
                  updateModel.get.executorErrors = resultOfBlock._2._2
                }
              }
            }
          ))

        }
        else {
        val newStatusMap = scala.collection.mutable.Map.empty[String, String]
        if (status.nonEmpty) {
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
        } else {
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
        }

        if (loadStatus != CarbonCommonConstants.STORE_LOADSTATUS_FAILURE &&
            partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
          loadStatus = partitionStatus
        }
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
              if (ex.getCause != null) {
                executorMessage = ex.getCause.getMessage
                errorMessage = errorMessage + ": " + executorMessage
              }
          }
          LOGGER.info(errorMessage)
          LOGGER.error(ex)
      }
      // handle the status file updation for the update cmd.
      if (updateModel.isDefined) {

      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
      // updateModel.get.executorErrors.errorMsg = errorMessage
          if (updateModel.get.executorErrors.failureCauses == FailureCauses.NONE) {
            updateModel.get.executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
            updateModel.get.executorErrors.errorMsg = "Update failed as the data load has failed."
          }
          return
        }
        else {
          // in success case handle updation of the table status file.
          // success case.
          val segmentDetails = new util.HashSet[String]()

          var resultSize = 0

          res.foreach(resultOfSeg => {
            resultSize = resultSize + resultOfSeg.size
            resultOfSeg.foreach(
            resultOfBlock => {
              segmentDetails.add(resultOfBlock._2._1.getLoadName)
            }
          )}
          )

          // this means that the update doesnt have any records to update so no need to do table
          // status file updation.
          if (resultSize == 0) {
            LOGGER.audit("Data update is successful with 0 rows updation for " +
                         s"${carbonLoadModel.getDatabaseName}.${carbonLoadModel.getTableName}")
            return
          }

          if (
            CarbonUpdateUtil
              .updateTableMetadataStatus(segmentDetails,
                carbonTable,
                updateModel.get.updatedTimeStamp + "",
                true,
                new util.ArrayList[String](0))) {
            LOGGER.audit("Data update is successful for " +
                         s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
          }
          else {
            val errorMessage = "Data update failed due to failure in table status updation."
            LOGGER.audit("Data update is failed for " +
                         s"${carbonLoadModel.getDatabaseName}.${carbonLoadModel.getTableName}")
            LOGGER.error("Data update failed due to failure in table status updation.")
            updateModel.get.executorErrors.errorMsg = errorMessage
            updateModel.get.executorErrors.failureCauses = FailureCauses
              .STATUS_FILE_UPDATION_FAILURE
            return
          }

        }

        return
      }
        LOGGER.info("********starting clean up**********")
      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
        CarbonLoaderUtil.deleteSegment(carbonLoadModel, currentLoadCount)
        LOGGER.info("********clean up done**********")
        LOGGER.audit(s"Data load is failed for " +
            s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.warn("Cannot write load metadata file as data load failed")
        shutdownDictionaryServer(carbonLoadModel, result, false)
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
            shutdownDictionaryServer(carbonLoadModel, result, false)
            throw new Exception(errorMessage)
          }
        } else if (!carbonLoadModel.isRetentionRequest) {
          // TODO : Handle it
          LOGGER.info("********Database updated**********")
        }

        shutdownDictionaryServer(carbonLoadModel, result)
        if (CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS.equals(loadStatus)) {
          LOGGER.audit("Data load is partially successful for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        } else {
          LOGGER.audit("Data load is successful for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        }
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

  private def shutdownDictionaryServer(carbonLoadModel: CarbonLoadModel,
      result: Future[DictionaryServer], writeDictionary: Boolean = true) = {
    // write dictionary file and shutdown dictionary server
    if (carbonLoadModel.getUseOnePass) {
      try {
        val server = result.get()
        if (writeDictionary) {
          server.writeDictionary()
        }
        server.shutdown()
      } catch {
        case ex: Exception =>
          LOGGER.error("Error while close dictionary server and write dictionary file for " +
                       s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
          throw new Exception("Dataload failed due to error while write dictionary file!")
      }
    }
  }
}
