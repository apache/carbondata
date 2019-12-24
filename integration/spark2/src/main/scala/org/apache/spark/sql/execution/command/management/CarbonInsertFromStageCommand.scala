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
import java.util.concurrent.{Executors, ExecutorService}

import scala.collection.JavaConverters._

import com.google.gson.Gson
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager, StageInput}
import org.apache.carbondata.core.util.path.CarbonTablePath
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
    tableName: String
) extends DataCommand {

  @transient var LOGGER: Logger = _

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
    var loadModel: CarbonLoadModel = null
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
      val stageFiles = listStageFiles(stagePath, hadoopConf)
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

      // 3) add new segment with INSERT_IN_PROGRESS into table status
      loadModel = DataLoadProcessBuilderOnSpark.createLoadModelForGlobalSort(spark, table)
      CarbonLoaderUtil.recordNewLoadMetadata(loadModel)

      // 4) write all existing stage file names and segmentId into a new snapshot file
      // The content of snapshot file is: first line is segmentId, followed by each line is
      // one stage file name
      val content =
        (Seq(loadModel.getSegmentId) ++ stageFiles.map(_._1.getAbsolutePath)).mkString("\n")
      FileFactory.writeFile(content, snapshotFilePath)

      // 5) perform data loading
      startLoading(spark, table, loadModel, stageInputs)

      // 6) write segment file and update the segment entry to SUCCESS
      val segmentFileName = SegmentFileStore.writeSegmentFile(
        table, loadModel.getSegmentId, loadModel.getFactTimeStamp.toString)
      SegmentFileStore.updateTableStatusFile(
        table, loadModel.getSegmentId, segmentFileName,
        table.getCarbonTableIdentifier.getTableId,
        new SegmentFileStore(table.getTablePath, segmentFileName),
        SegmentStatus.SUCCESS)

      // 7) delete stage files
      deleteStageFiles(executorService, stageFiles)

      // 8) delete the snapshot file
      FileFactory.getCarbonFile(snapshotFilePath).delete()
    } catch {
      case ex: Throwable =>
        LOGGER.error(s"failed to insert ${table.getDatabaseName}.${table.getTableName}", ex)
        if (loadModel != null) {
          CarbonLoaderUtil.updateTableStatusForFailure(loadModel)
        }
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
    if (!FileFactory.isFileExist(snapshotFilePath)) {
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
        case SegmentStatus.INSERT_IN_PROGRESS =>
          // delete entry in table status and load again
          LOGGER.info(s"Segment $segmentId is in INSERT_IN_PROGRESS state, about to delete the " +
                      s"segment entry and load again")
          val segmentToWrite = segments.filterNot(_.getLoadName.equals(segmentId))
          SegmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(table.getTablePath),
            segmentToWrite)
        case other =>
          throw new RuntimeException(s"Segment $segmentId is in unexpected state: $other")
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
      loadModel: CarbonLoadModel,
      stageInput: Seq[StageInput]
  ): Unit = {
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
   */
  private def deleteStageFiles(
      executorService: ExecutorService,
      stageFiles: Array[(CarbonFile, CarbonFile)]): Unit = {
    val startTime = System.currentTimeMillis()
    stageFiles.map { files =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          files._1.delete()
          files._2.delete()
        }
      })
    }.map { future =>
      future.get()
    }
    LOGGER.info(s"finished to delete stage files, time taken: " +
                s"${System.currentTimeMillis() - startTime}ms")
  }

  /*
   * Collect all stage files and matched success files.
   * A stage file without success file will not be collected
   */
  private def listStageFiles(
      loadDetailsDir: String,
      hadoopConf: Configuration
  ): Array[(CarbonFile, CarbonFile)] = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir, hadoopConf)
    if (dir.exists()) {
      val allFiles = dir.listFiles()
      val successFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap
      allFiles.filter { file =>
        !file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUBFIX)
      }.filter { file =>
        successFiles.contains(file.getName)
      }.map { file =>
        (file, successFiles(file.getName))
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
