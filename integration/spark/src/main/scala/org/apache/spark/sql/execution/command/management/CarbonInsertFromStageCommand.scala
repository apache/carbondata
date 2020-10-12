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

package org.apache.spark.sql.execution.command.management

import java.io.{DataInputStream, File, InputStreamReader, IOException}
import java.util
import java.util.Collections
import java.util.concurrent.{Callable, Executors, ExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{ColumnarFormatVersion, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager, StageInput}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePostStatusUpdateEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory

/**
 * Collect stage input files and trigger a loading into carbon table.
 *
 * @param databaseNameOp database name
 * @param tableName table name
 */
case class CarbonInsertFromStageCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String]
) extends DataCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processData(spark: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, spark)
    val table = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(spark)
    val hadoopConf = spark.sessionState.newHadoopConf()
    FileFactory.getConfiguration.addResource(hadoopConf)
    setAuditTable(table)

    if (!table.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (table.isMV) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV table")
    }

    val tablePath = table.getTablePath
    val stagePath = CarbonTablePath.getStageDir(tablePath)
    val snapshotFilePath = CarbonTablePath.getStageSnapshotFile(tablePath)
    var stageFiles: Array[(CarbonFile, CarbonFile)] = Array.empty
    var executorService: ExecutorService = null
    val lock = acquireIngestLock(table)

    try {
      // 1. Check whether we need to recover from previous failure
      // We use a snapshot file to indicate whether there was failure in previous
      // ingest operation. A Snapshot file will be created when an ingest operation
      // starts and will be deleted only after the whole ingest operation is finished,
      // which includes two actions:
      //   1) action1: changing segment status to SUCCESS and
      //   2) action2: deleting all involved stage files.
      //
      // If one of these two actions is failed, the snapshot file will be exist, so
      // that recovery is needed.
      //
      // To do the recovery, do following steps:
      //   1) Check if corresponding segment in table status is SUCCESS,
      //      means deleting stage files had failed. So need to read the stage
      //      file list from the snapshot file and delete them again.
      //   2) Check if corresponding segment in table status is INSERT_IN_PROGRESS,
      //      means data loading had failed. So need to read the stage file list
      //      from the snapshot file and load again.
      recoverIfRequired(snapshotFilePath, table, hadoopConf)

      // 2. Start ingesting, steps:
      //   1) read all existing stage files
      //   2) read all stage files to collect input files for data loading
      //   3) add a new segment entry in table status as INSERT_IN_PROGRESS,
      //   4) write all existing stage file names into a new snapshot file
      //   5) do actual loading
      //   6) write segment file and update segment state to SUCCESS in table status
      //   7) delete stage files used for loading
      //   8) delete the snapshot file

      // 1) read all existing stage files
      val batchSize = try {
        Integer.valueOf(
          options.getOrElse(CarbonInsertFromStageCommand.BATCH_FILE_COUNT_KEY,
            CarbonInsertFromStageCommand.BATCH_FILE_COUNT_DEFAULT))
      } catch {
        case _: NumberFormatException =>
          throw new MalformedCarbonCommandException("Option [" +
            CarbonInsertFromStageCommand.BATCH_FILE_COUNT_KEY + "] is not a number.")
      }
      if (batchSize < 1) {
        throw new MalformedCarbonCommandException("Option [" +
            CarbonInsertFromStageCommand.BATCH_FILE_COUNT_KEY + "] is less than 1.")
      }
      val orderType = options.getOrElse(CarbonInsertFromStageCommand.BATCH_FILE_ORDER_KEY,
        CarbonInsertFromStageCommand.BATCH_FILE_ORDER_DEFAULT)
      if (!orderType.equalsIgnoreCase(CarbonInsertFromStageCommand.BATCH_FILE_ORDER_ASC) &&
        !orderType.equalsIgnoreCase(CarbonInsertFromStageCommand.BATCH_FILE_ORDER_DESC)) {
        throw new MalformedCarbonCommandException("Option [" +
            CarbonInsertFromStageCommand.BATCH_FILE_ORDER_KEY + "] is invalid, should be " +
            CarbonInsertFromStageCommand.BATCH_FILE_ORDER_ASC + " or " +
            CarbonInsertFromStageCommand.BATCH_FILE_ORDER_DESC + ".")
      }
      LOGGER.info("Option [" + CarbonInsertFromStageCommand.BATCH_FILE_ORDER_KEY +
                  "] value is " + orderType)
      stageFiles = listStageFiles(stagePath, hadoopConf, batchSize,
        orderType.equalsIgnoreCase(CarbonInsertFromStageCommand.BATCH_FILE_ORDER_ASC))
      if (stageFiles.isEmpty) {
        // no stage files, so do nothing
        LOGGER.warn("files not found under stage metadata folder")
        return Seq.empty
      }

      // We add a tag 'loading' to the stages in process.
      // different insert stage processes can load different data separately
      // by choose the stages without 'loading' tag or stages loaded timeout.
      // which avoid loading the same data between concurrent insert stage processes.
      // The 'loading' tag is actually an empty file with
      // '.loading' suffix filename
      val numThreads = Math.min(Math.max(stageFiles.length, 1), 10)
      executorService = Executors.newFixedThreadPool(numThreads)
      createStageLoadingFilesWithRetry(table.getStagePath, executorService, stageFiles)
    } catch {
      case ex: Throwable =>
        LOGGER.error(s"failed to insert ${table.getDatabaseName}.${table.getTableName}", ex)
        shutdownExecutorService(executorService)
        throw ex
    } finally {
      lock.unlock()
    }

    try{
      // 2) read all stage files to collect input files for data loading
      // create a thread pool to read them
      val stageInputs = collectStageInputs(executorService, stagePath, stageFiles)

      // 3) perform data loading
      if (table.isHivePartitionTable) {
        startLoadingWithPartition(spark, table, stageInputs, stageFiles, snapshotFilePath)
      } else {
        startLoading(spark, table, stageInputs, stageFiles, snapshotFilePath)
      }

      // 4) delete stage files
      deleteStageFilesWithRetry(table.getStagePath, executorService, stageFiles)

      // 5) delete the snapshot file
      deleteSnapShotFileWithRetry(table, snapshotFilePath)
    } catch {
      case ex: Throwable =>
        LOGGER.error(s"failed to insert ${table.getDatabaseName}.${table.getTableName}", ex)
        throw ex
    } finally {
      shutdownExecutorService(executorService)
    }
    Seq.empty
  }

  /**
   * Check whether there was failure in previous ingest process and try to recover
   */
  private def recoverIfRequired(
      snapshotFilePath: String,
      table: CarbonTable,
      conf: Configuration): Unit = {
    if (!FileFactory.isFileExist(snapshotFilePath) || table.isHivePartitionTable) {
      // everything is fine
      return
    }

    // something wrong, read the snapshot file and do recover steps
    // 1. check segment state in table status file
    // 2. If in SUCCESS state, delete all stage files read inn snapshot file
    // 3. If in IN_PROGRESS state, delete the entry in table status and load again
    LOGGER.info(s"snapshot file found ($snapshotFilePath), start recovery process")
    val lines = FileFactory.readLinesInFile(snapshotFilePath, conf)
    if (lines.size() < 2) {
      throw new RuntimeException("Invalid snapshot file, " + lines.size() + " lines")
    }

    val segmentId = lines.get(0)
    val stageFileNames = lines.remove(0)
    LOGGER.info(s"Segment $segmentId need recovery, ${stageFileNames.length} stage files")

    // lock the table status
    var lock = CarbonLockFactory.getCarbonLockObj(
      table.getAbsoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
    if (!lock.lockWithRetries()) {
      throw new RuntimeException(s"Failed to lock table status for " +
        s"${table.getDatabaseName}.${table.getTableName}")
    }
    var executorService: ExecutorService = null
    try {
      val segments = SegmentStatusManager.readTableStatusFile(
        CarbonTablePath.getTableStatusFilePath(table.getTablePath)
      )
      val matchedSegment = segments.filter(_.getLoadName.equals(segmentId))
      if (matchedSegment.length != 1) {
        throw new RuntimeException("unexpected " + matchedSegment.length + " segment found")
      }
      matchedSegment(0).getSegmentStatus match {
        case SegmentStatus.SUCCESS =>
          // delete all stage files
          lock.unlock()
          lock = null
          LOGGER.info(s"Segment $segmentId is in SUCCESS state, about to delete " +
            s"${stageFileNames.length} stage files")
          val numThreads = Math.min(Math.max(stageFileNames.length, 1), 10)
          executorService = Executors.newFixedThreadPool(numThreads)
          stageFileNames.map { fileName =>
            executorService.submit(new Runnable {
              override def run(): Unit = {
                FileFactory.getCarbonFile(
                  CarbonTablePath.getStageDir(table.getTablePath) +
                    CarbonCommonConstants.FILE_SEPARATOR + fileName
                ).delete()
              }
            })
          }.map { future =>
            future.get()
          }
        case other =>
          // delete entry in table status and load again
          LOGGER.warn(s"Segment $segmentId is in $other state, about to delete the " +
            s"segment entry and load again")
          val segmentToWrite = segments.filterNot(_.getLoadName.equals(segmentId))
          SegmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(table.getTablePath),
            segmentToWrite)
      }
    } finally {
      if (lock != null) {
        lock.unlock()
      }
      shutdownExecutorService(executorService)
    }
    LOGGER.info(s"Finish recovery, delete snapshot file: $snapshotFilePath")
    FileFactory.getCarbonFile(snapshotFilePath).delete()
  }

  /**
   * Start global sort loading
   */
  private def startLoading(
      spark: SparkSession,
      table: CarbonTable,
      stageInput: Seq[StageInput],
      stageFiles: Array[(CarbonFile, CarbonFile)],
      snapshotFilePath: String
  ): Unit = {
    var loadModel: CarbonLoadModel = null
    try {
      // 1) add new segment with INSERT_IN_PROGRESS into table status
      loadModel = DataLoadProcessBuilderOnSpark.createLoadModelForGlobalSort(spark, table)
      CarbonLoaderUtil.recordNewLoadMetadata(loadModel)

      // 2) write all existing stage file names and segmentId into a new snapshot file
      // The content of snapshot file is: first line is segmentId, followed by each line is
      // one stage file name
      val content =
      (Seq(loadModel.getSegmentId) ++ stageFiles.map(_._1.getAbsolutePath)).mkString("\n")
      FileFactory.writeFile(content, snapshotFilePath)

      // 3) do loading.
      val splits = stageInput.flatMap(_.createSplits().asScala)
      LOGGER.info(s"start to load ${splits.size} files into " +
                  s"${table.getDatabaseName}.${table.getTableName}")
      val start = System.currentTimeMillis()
      val dataFrame = DataLoadProcessBuilderOnSpark.createInputDataFrame(spark, table, splits)
      // accumulator to collect segment metadata info such as columnId and it's minMax values
      val segmentMetaDataAccumulator = spark.sqlContext
        .sparkContext
        .collectionAccumulator[Map[String, SegmentMetaDataInfo]]
      if (table.getBucketingInfo == null) {
        DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(
          spark,
          Option(dataFrame),
          loadModel,
          SparkSQLUtil.sessionState(spark).newHadoopConf(),
          segmentMetaDataAccumulator
        ).map { row =>
          (row._1, FailureCauses.NONE == row._2._2.failureCauses)
        }
      } else {
        CarbonDataRDDFactory.loadDataFrame(
          spark.sqlContext,
          Option(dataFrame),
          None,
          loadModel,
          segmentMetaDataAccumulator)
      }
      LOGGER.info(s"finish data loading, time taken ${System.currentTimeMillis() - start}ms")

      // 4) write segment file and update the segment entry to SUCCESS
      // create operationContext to fire load events
      val operationContext: OperationContext = new OperationContext
      val (tableIndexes, indexOperationContext) = CommonLoadUtils.firePreLoadEvents(
        sparkSession = spark,
        carbonLoadModel = loadModel,
        uuid = "",
        factPath = "",
        optionsFinal = options.asJava,
        options = options.asJava,
        isOverwriteTable = false,
        isDataFrame = true,
        updateModel = None,
        operationContext = operationContext)
      // in case of insert stage files, added the below property to avoid merge index and
      // fire event to load data to secondary index
      val loadTablePreStatusUpdateEvent: LoadTablePreStatusUpdateEvent =
        new LoadTablePreStatusUpdateEvent(
          table.getCarbonTableIdentifier,
          loadModel)
      OperationListenerBus.getInstance().fireEvent(loadTablePreStatusUpdateEvent, operationContext)

      val segmentFileName = SegmentFileStore.writeSegmentFile(
        table, loadModel.getSegmentId, loadModel.getFactTimeStamp.toString)

      val status = SegmentFileStore.updateTableStatusFile(
        table, loadModel.getSegmentId, segmentFileName,
        table.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(table.getTablePath, segmentFileName),
        SegmentStatus.SUCCESS)

      // trigger load post events
      if (status) {
        val loadTablePostStatusUpdateEvent: LoadTablePostStatusUpdateEvent =
          new LoadTablePostStatusUpdateEvent(loadModel)
        try {
          OperationListenerBus.getInstance()
            .fireEvent(loadTablePostStatusUpdateEvent, operationContext)
        } catch {
          case ex: Exception =>
            LOGGER.error("Problem while committing indexes", ex)
        }
      }
      // fire event to load data to materialized views and merge bloom index files
      CommonLoadUtils.firePostLoadEvents(spark,
        loadModel,
        tableIndexes,
        indexOperationContext,
        table,
        operationContext)
    } catch {
      case ex: Throwable =>
        LOGGER.error(s"failed to insert ${table.getDatabaseName}.${table.getTableName}", ex)
        if (loadModel != null) {
          CarbonLoaderUtil.updateTableStatusForFailure(loadModel)
        }
        throw ex
    }
  }

  /**
   * Start global sort loading of partition table
   */
  private def startLoadingWithPartition(
      spark: SparkSession,
      table: CarbonTable,
      stageInput: Seq[StageInput],
      stageFiles: Array[(CarbonFile, CarbonFile)],
      snapshotFilePath: String
    ): Unit = {
    val partitionDataList = listPartitionFiles(stageInput)

    val start = System.currentTimeMillis()
    partitionDataList.map {
      case (partition, splits) =>
        LOGGER.info(s"start to load ${splits.size} files into " +
          s"${table.getDatabaseName}.${table.getTableName}. " +
          s"Partition information: ${partition.mkString(",")}")
        val dataFrame =
          DataLoadProcessBuilderOnSpark.createInputDataFrame(spark, table, splits)
        val columns = dataFrame.columns
        val header = columns.mkString(",")
        val selectColumns = columns.filter(!partition.contains(_))
        val selectedDataFrame = dataFrame.select(selectColumns.head, selectColumns.tail: _*)
        CarbonInsertIntoCommand(
          databaseNameOp = Option(table.getDatabaseName),
          tableName = table.getTableName,
          options = scala.collection.immutable.Map("fileheader" -> header,
            "binary_decoder" -> "base64"),
          isOverwriteTable = false,
          logicalPlan = selectedDataFrame.queryExecution.analyzed,
          tableInfo = table.getTableInfo,
          partition = partition
        ).run(spark)
    }
    LOGGER.info(s"finish data loading, time taken ${ System.currentTimeMillis() - start }ms")
  }

  /**
   * @return return a (partitionMap, InputSplits) pair list.
   *         the partitionMap contains all partition column name and value.
   *         the InputSplits is all data file information of current partition.
   */
  private def listPartitionFiles(
      stageInputs : Seq[StageInput]
    ): Seq[(Map[String, Option[String]], Seq[InputSplit])] = {
    val partitionMap = new util.HashMap[Map[String, Option[String]], util.List[InputSplit]]()
    stageInputs.foreach (
      stageInput => {
        val locations = stageInput.getLocations.asScala
        locations.foreach (
          location => {
            val partition = location.getPartitions.asScala.map(t => (t._1, Option(t._2))).toMap
            var splits = partitionMap.get(partition)
            if (splits == null) {
              partitionMap.put(partition, new util.ArrayList[InputSplit]())
              splits = partitionMap.get(partition)
            }
            splits.addAll (
              location.getFiles.asScala
              .filter(_._1.endsWith(CarbonCommonConstants.FACT_FILE_EXT))
              .map(
                file => {
                  CarbonInputSplit.from(
                    "-1", "0",
                    stageInput.getBase + CarbonCommonConstants.FILE_SEPARATOR + file._1, 0,
                    file._2, ColumnarFormatVersion.V3, null
                  )
                }
              ).toList.asJava
            )
          }
        )
      }
    )
    partitionMap.asScala.map(entry => (entry._1, entry._2.asScala)).toSeq
  }

  /**
   * Read stage files and return input files
   */
  private def collectStageInputs(
      executorService: ExecutorService,
      tableStagePath: String,
      stageFiles: Array[(CarbonFile, CarbonFile)]
  ): Seq[StageInput] = {
    val startTime = System.currentTimeMillis()
    val output = Collections.synchronizedList(new util.ArrayList[StageInput]())
    val gson = new Gson()
    stageFiles.map { stage =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val filePath = tableStagePath + CarbonCommonConstants.FILE_SEPARATOR + stage._1.getName
          var stream: DataInputStream = null
          try {
            stream = FileFactory.getDataInputStream(filePath)
            var retry = CarbonInsertFromStageCommand.DELETE_FILES_RETRY_TIMES
            breakable (
              while (retry > 0) {
                try {
                  val stageInput = gson.fromJson(new InputStreamReader(stream), classOf[StageInput])
                  output.add(stageInput)
                  break()
                } catch {
                  case ex: Exception => retry -= 1
                    if (retry > 0) {
                      LOGGER.warn(s"The stage file $filePath can't be read, retry " +
                        s"$retry times: ${ex.getMessage}")
                      Thread.sleep(CarbonInsertFromStageCommand.DELETE_FILES_RETRY_INTERVAL)
                    } else {
                      LOGGER.error(s"The stage file $filePath can't be read: ${ex.getMessage}")
                      throw ex
                    }
                }
              }
            )
          } finally {
            if (stream != null) {
              stream.close()
            }
          }
        }
      })
    }.map { future =>
      future.get()
    }
    LOGGER.info(s"read stage files taken ${System.currentTimeMillis() - startTime}ms")
    output.asScala
  }

  /**
   * create '.loading' file to tag the stage in process
   * return the loading files failed to create
   */
  private def createStageLoadingFiles(
      stagePath: String,
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Array[(CarbonFile, CarbonFile)] = {
    stageFiles.map { files =>
      executorService.submit(new Callable[(CarbonFile, CarbonFile, Boolean)] {
        override def call(): (CarbonFile, CarbonFile, Boolean) = {
          try {
            // Get the loading files path
            val stageLoadingFile =
              FileFactory.getCarbonFile(stagePath +
                File.separator + files._1.getName + CarbonTablePath.LOADING_FILE_SUFFIX);
            // Try to recreate loading files if the loading file exists
            // or create loading files directly if the loading file doesn't exist
            // set isFailed to be false when (delete and) createfile success
            val isFailed = if (stageLoadingFile.exists()) {
              !(stageLoadingFile.delete() && stageLoadingFile.createNewFile())
            } else {
              !stageLoadingFile.createNewFile()
            }
            (files._1, files._2, isFailed)
          } catch {
            case _ : Exception => (files._1, files._2, true)
          }
        }
      })
    }.map { future =>
      future.get()
    }.filter { files =>
      // keep the files when isFailed is true. so we can retry on these files.
      files._3
    }.map { files =>
      (files._1, files._2)
    }
  }

  /**
   * create '.loading' file with retry
   */
  private def createStageLoadingFilesWithRetry(
      stagePath: String,
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Unit = {
    val startTime = System.currentTimeMillis()
    var retry = CarbonInsertFromStageCommand.DELETE_FILES_RETRY_TIMES
    var needToCreateStageLoadingFiles = stageFiles
    while (retry > 0 && needToCreateStageLoadingFiles.nonEmpty) {
      needToCreateStageLoadingFiles =
        createStageLoadingFiles(stagePath, executorService, needToCreateStageLoadingFiles)
      retry -= 1
    }
    LOGGER.info(s"finished to create stage loading files, time taken: " +
      s"${System.currentTimeMillis() - startTime}ms")
    if (needToCreateStageLoadingFiles.nonEmpty) {
      LOGGER.warn(s"failed to create loading files:" +
        needToCreateStageLoadingFiles.map(_._1.getName).mkString(","))
    }
  }

  /**
   * Delete stage files and success files and loading files
   * Return the files failed to delete
   */
  private def deleteStageFiles(
      stagePath: String,
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Array[(CarbonFile, CarbonFile)] = {
    stageFiles.map { files =>
      executorService.submit(new Callable[(CarbonFile, CarbonFile, Boolean)] {
        override def call(): (CarbonFile, CarbonFile, Boolean) = {
          // Delete three types of file: stage|.success|.loading
          try {
            val stageLoadingFile = FileFactory.getCarbonFile(stagePath +
              File.separator + files._1.getName + CarbonTablePath.LOADING_FILE_SUFFIX);
            var isFailed = false
            // If delete() return false, maybe the reason is FileNotFount or FileFailedClean.
            // Considering FileNotFound means FileCleanSucessfully.
            // We need double check the file exists or not when delete() return false.
            if (!files._1.delete() || !files._2.delete() || !stageLoadingFile.delete()) {
              // If the file still exists,  make isFailed to be true
              // So we can retry to delete this file.
              isFailed = files._1.exists() || files._1.exists() || stageLoadingFile.exists()
            }
            (files._1, files._2, isFailed)
          } catch {
            case _: Exception => (files._1, files._2, true)
          }
        }
      })
    }.map { future =>
      future.get()
    }.filter { files =>
      // keep the files when isFailed is true. so we can retry on these files.
      files._3
    }.map { files =>
      (files._1, files._2)
    }
  }

  /**
   * Delete stage file and success file with retry
   */
  private def deleteStageFilesWithRetry(
      stagePath: String,
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Unit = {
    val startTime = System.currentTimeMillis()
    var retry = CarbonInsertFromStageCommand.DELETE_FILES_RETRY_TIMES
    var needToDeleteStageFiles = stageFiles
    while (retry > 0 && needToDeleteStageFiles.nonEmpty) {
      needToDeleteStageFiles =
        deleteStageFiles(stagePath, executorService, needToDeleteStageFiles)
      retry -= 1
    }
    LOGGER.info(s"finished to delete stage files, time taken: " +
      s"${System.currentTimeMillis() - startTime}ms")
    // if there are still stage files failed to clean, print log.
    if (needToDeleteStageFiles.nonEmpty) {
      LOGGER.warn(s"failed to clean up stage files:" +
        needToDeleteStageFiles.map(_._1.getName).mkString(","))
    }
  }

  /**
   * Delete snapshot file with retry
   * Return false means the snapshot file was cleaned successfully
   * While return true means the snapshot file was failed to clean
   */
  private def deleteSnapShotFile(
      snapshotFilePath: String): Boolean = {
    val snapshotFile = FileFactory.getCarbonFile(snapshotFilePath)
    // If delete() return false, maybe the reason is FileNotFount or FileFailedClean.
    // Considering FileNotFound means file clean successfully.
    // We need double check the file exists or not when delete() return false.
    if (!snapshotFile.delete()) {
      return snapshotFile.exists()
    }
    true
  }

  /**
   * Delete snapshot file with retry
   */
  private def deleteSnapShotFileWithRetry(
      table: CarbonTable,
      snapshotFilePath: String): Unit = {
    if (table.isHivePartitionTable) {
      return
    }
    var retries = CarbonInsertFromStageCommand.DELETE_FILES_RETRY_TIMES
    while(deleteSnapShotFile(snapshotFilePath) && retries > 0) {
      retries -= 1
    }
  }

  /*
   * Collect all stage files and matched success files.
   * A stage file without success file will not be collected
   */
  private def listStageFiles(
      loadDetailsDir: String,
      hadoopConf: Configuration,
      batchSize: Int,
      ascendingSort: Boolean
  ): Array[(CarbonFile, CarbonFile)] = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir, hadoopConf)
    if (dir.exists()) {
      val allFiles = dir.listFiles()
      val successFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUFFIX)
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap

      // different insert stage processes can load different data separately
      // by choose the stages without 'loading' tag or stages loaded timeout.
      // which avoid loading the same data between concurrent insert stage processes.
      // Overall, There are two conditions to choose stages to process:
      // 1) stages never loaded, choose the stages without '.loading' tag.
      // 2) stages loaded timeout, the timeout threshold depends on INSERT_STAGE_TIMEOUT
      val loadingFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.LOADING_FILE_SUFFIX)
      }.filter { file =>
        (System.currentTimeMillis() - file.getLastModifiedTime) <
          CarbonInsertFromStageCommand.INSERT_STAGE_TIMEOUT
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap

      val stageFiles = allFiles.filter { file =>
        !file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUFFIX)
      }.filter { file =>
        !file.getName.endsWith(CarbonTablePath.LOADING_FILE_SUFFIX)
      }.filter { file =>
        successFiles.contains(file.getName)
      }.filterNot { file =>
        loadingFiles.contains(file.getName)
      }.sortWith {
        (file1, file2) =>
          if (ascendingSort) {
            file1.getLastModifiedTime < file2.getLastModifiedTime
          } else {
            file1.getLastModifiedTime > file2.getLastModifiedTime
          }
      }.map { file =>
        (file, successFiles(file.getName))
      }
      if (stageFiles.length <= batchSize) {
        stageFiles
      } else {
        stageFiles.dropRight(stageFiles.length - batchSize)
      }
    } else {
      Array.empty
    }
  }

  /**
   * INGEST operation does not support concurrent, so it is one lock for one table
   */
  private def acquireIngestLock(table: CarbonTable): ICarbonLock = {
    val tableIdentifier = table.getAbsoluteTableIdentifier
    val lock = CarbonLockFactory.getCarbonLockObj(tableIdentifier, LockUsage.INGEST_LOCK)
    val retryCount = CarbonLockUtil.getLockProperty(
      CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK,
      CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK_DEFAULT
    )
    val maxTimeout = CarbonLockUtil.getLockProperty(
      CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
      CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT
    )
    if (lock.lockWithRetries(retryCount, maxTimeout)) {
      lock
    } else {
      throw new IOException(
        s"Not able to acquire the lock for table status file for $tableIdentifier")
    }
  }

  private def shutdownExecutorService(executorService: ExecutorService): Unit = {
    if (executorService != null && !executorService.isShutdown) {
      executorService.shutdownNow()
    }
  }

  override protected def opName: String = "INSERT STAGE"
}

object CarbonInsertFromStageCommand {

  val DELETE_FILES_RETRY_TIMES = 3

  val DELETE_FILES_RETRY_INTERVAL = 1000

  val BATCH_FILE_COUNT_KEY = "batch_file_count"

  val BATCH_FILE_COUNT_DEFAULT: String = Integer.MAX_VALUE.toString

  val BATCH_FILE_ORDER_KEY = "batch_file_order"

  /**
   * Use this option will insert the earliest stage files into the table.
   */
  val BATCH_FILE_ORDER_ASC = "ASC"

  /**
   * Use this option will insert the latest stage files into the table.
   */
  val BATCH_FILE_ORDER_DESC = "DESC"

  /*
  * Keep default ascending order. (Earliest first)
  */
  val BATCH_FILE_ORDER_DEFAULT: String = BATCH_FILE_ORDER_ASC

  val INSERT_STAGE_TIMEOUT = CarbonProperties.getInsertStageTimeout
}
