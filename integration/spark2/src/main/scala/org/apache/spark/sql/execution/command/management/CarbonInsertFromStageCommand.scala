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

import java.io.{InputStreamReader, IOException}
import java.util
import java.util.Collections
import java.util.concurrent.{Callable, Executors, ExecutorService}

import scala.collection.JavaConverters._

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{AbstractDFSCarbonFile, CarbonFile}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{ColumnarFormatVersion, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager, StageInput}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark

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

  @transient var LOGGER: Logger = _

  val DELETE_FILES_RETRY_TIMES = 3

  override def processData(spark: SparkSession): Seq[Row] = {
    LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    Checker.validateTableExists(databaseNameOp, tableName, spark)
    val table = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(spark)
    val hadoopConf = spark.sessionState.newHadoopConf()
    FileFactory.getConfiguration.addResource(hadoopConf)
    setAuditTable(table)

    if (!table.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (table.isChildTableForMV) {
      throw new MalformedCarbonCommandException("Unsupported operation on MV table")
    }

    val tablePath = table.getTablePath
    val stagePath = CarbonTablePath.getStageDir(tablePath)
    val snapshotFilePath = CarbonTablePath.getStageSnapshotFile(tablePath)
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
        Integer.valueOf(options.getOrElse("batch_file_count", Integer.MAX_VALUE.toString))
      } catch {
        case _: NumberFormatException =>
          throw new MalformedCarbonCommandException("Option [batch_file_count] is not a number.")
      }
      if (batchSize < 1) {
        throw new MalformedCarbonCommandException("Option [batch_file_count] is less than 1.")
      }
      val stageFiles = listStageFiles(stagePath, hadoopConf, batchSize)
      if (stageFiles.isEmpty) {
        // no stage files, so do nothing
        LOGGER.warn("files not found under stage metadata folder")
        return Seq.empty
      }

      // 2) read all stage files to collect input files for data loading
      // create a thread pool to read them
      val numThreads = Math.min(Math.max(stageFiles.length, 1), 10)
      val executorService = Executors.newFixedThreadPool(numThreads)
      val stageInputs = collectStageInputs(executorService, stageFiles)

      // 3) perform data loading
      if (table.isHivePartitionTable) {
        startLoadingWithPartition(spark, table, stageInputs, stageFiles, snapshotFilePath)
      } else {
        startLoading(spark, table, stageInputs, stageFiles, snapshotFilePath)
      }

      // 4) delete stage files
      deleteStageFilesWithRetry(executorService, stageFiles)

      // 5) delete the snapshot file
      deleteSnapShotFileWithRetry(table, snapshotFilePath)
    } catch {
      case ex: Throwable =>
        LOGGER.error(s"failed to insert ${table.getDatabaseName}.${table.getTableName}", ex)
        throw ex
    } finally {
      lock.unlock()
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
          val executorService = Executors.newFixedThreadPool(numThreads)
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
      DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(
        spark,
        Option(dataFrame),
        loadModel,
        SparkSQLUtil.sessionState(spark).newHadoopConf()
      ).map { row =>
        (row._1, FailureCauses.NONE == row._2._2.failureCauses)
      }
      LOGGER.info(s"finish data loading, time taken ${System.currentTimeMillis() - start}ms")

      // 4) write segment file and update the segment entry to SUCCESS
      val segmentFileName = SegmentFileStore.writeSegmentFile(
        table, loadModel.getSegmentId, loadModel.getFactTimeStamp.toString)
      SegmentFileStore.updateTableStatusFile(
        table, loadModel.getSegmentId, segmentFileName,
        table.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(table.getTablePath, segmentFileName),
        SegmentStatus.SUCCESS)
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
        CarbonInsertIntoWithDf(
          databaseNameOp = Option(table.getDatabaseName),
          tableName = table.getTableName,
          options = scala.collection.immutable.Map("fileheader" -> header,
            "binary_decoder" -> "base64"),
          isOverwriteTable = false,
          dataFrame = selectedDataFrame,
          updateModel = None,
          tableInfoOp = None,
          internalOptions = Map.empty,
          partition = partition
        ).process(spark)
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
      stageFiles: Array[(CarbonFile, CarbonFile)]
  ): Seq[StageInput] = {
    val startTime = System.currentTimeMillis()
    val output = Collections.synchronizedList(new util.ArrayList[StageInput]())
    val gson = new Gson()
    stageFiles.map { stage =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val filePath = stage._1.getAbsolutePath
          val stream = FileFactory.getDataInputStream(filePath)
          try {
            val stageInput = gson.fromJson(new InputStreamReader(stream), classOf[StageInput])
            output.add(stageInput)
          } finally {
            stream.close()
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
   * Delete stage file and success file
   * Return false means the stage files were cleaned successfully
   * While return true means the stage files were failed to clean
   */
  private def deleteStageFiles(
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Array[(CarbonFile, CarbonFile)] = {
    stageFiles.map { files =>
      executorService.submit(new Callable[Boolean] {
        override def call(): Boolean = {
          // If delete() return false, maybe the reason is FileNotFount or FileFailedClean.
          // Considering FileNotFound means FileCleanSucessfully.
          // We need double check the file exists or not when delete() return false.
          if (!(files._1.delete() && files._2.delete())) {
            // If the file still exists, return ture, let the file filtered in.
            // So we can retry to delete this file.
            return files._1.exists() || files._1.exists()
          }
          // When delete successfully, return false, let the file filtered away.
          false
        }
      })
    }.filter { future =>
      future.get()
    }
    stageFiles
  }

  /**
   * Delete stage file and success file with retry
   */
  private def deleteStageFilesWithRetry(
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Unit = {
    val startTime = System.currentTimeMillis()
    var retry = DELETE_FILES_RETRY_TIMES
    while (deleteStageFiles(executorService, stageFiles).length > 0 && retry > 0) {
      retry -= 1
    }
    LOGGER.info(s"finished to delete stage files, time taken: " +
      s"${System.currentTimeMillis() - startTime}ms")
    // if there are still stage files failed to clean, print log.
    if (stageFiles.length > 0) {
      LOGGER.warn(s"failed to clean up stage files:" + stageFiles.map(_._1.getName).mkString(","))
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
    // Considering FileNotFound means FileCleanSucessfully.
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
    var retries = DELETE_FILES_RETRY_TIMES
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
      batchSize: Int
  ): Array[(CarbonFile, CarbonFile)] = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir, hadoopConf)
    if (dir.exists()) {
      // Only HDFS/OBS/S3 server side can guarantee the files got from iterator are sorted
      // based on file name so that we can use iterator to get the A and A.success together
      // without loop all files which can improve performance compared with list all files.
      // One file and another with '.success', so we need *2 as total and this value is just
      // an approximate value. For local files, as can it can we not guarantee the order, we
      // just list all.
      val allFiles = dir.listFiles(false, batchSize * 2)
      val successFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap
      val stageFiles = allFiles.filter { file =>
        !file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }.filter { file =>
        successFiles.contains(file.getName)
      }.sortWith {
        (file1, file2) => file1.getLastModifiedTime < file2.getLastModifiedTime
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

  override protected def opName: String = "INSERT STAGE"
}
