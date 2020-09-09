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

package org.apache.carbondata.cleanfiles

import java.sql.Timestamp
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.index.CarbonIndexUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonMetadata, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.{CarbonTablePath, TrashUtil}
import org.apache.carbondata.processing.loading.TableProcessingOperations

object CleanFilesUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * The method deletes all data if forceTableClean <true> and clean garbage segment
   * (MARKED_FOR_DELETE state) if forceTableClean <false>
   *
   * @param dbName                 : Database name
   * @param tableName              : Table name
   * @param tablePath              : Table path
   * @param carbonTable            : CarbonTable Object <null> in case of force clean
   * @param forceTableClean        : <true> for force clean it will delete all data
   *                               <false> it will clean garbage segment (MARKED_FOR_DELETE state)
   * @param currentTablePartitions : Hive Partitions  details
   */
  def cleanFiles(
    dbName: String,
    tableName: String,
    tablePath: String,
    carbonTable: CarbonTable,
    forceTableClean: Boolean,
    currentTablePartitions: Option[Seq[PartitionSpec]] = None,
    truncateTable: Boolean = false): Unit = {
    var carbonCleanFilesLock: ICarbonLock = null
    val absoluteTableIdentifier = if (forceTableClean) {
      AbsoluteTableIdentifier.from(tablePath, dbName, tableName, tableName)
    } else {
      carbonTable.getAbsoluteTableIdentifier
    }
    try {
      val errorMsg = "Clean files request is failed for " +
        s"$dbName.$tableName" +
        ". Not able to acquire the clean files lock due to another clean files " +
        "operation is running in the background."
      // in case of force clean the lock is not required
      if (forceTableClean) {
        FileFactory.deleteAllCarbonFilesOfDir(
          FileFactory.getCarbonFile(absoluteTableIdentifier.getTablePath))
      } else {
        carbonCleanFilesLock =
          CarbonLockUtil
            .getLockObject(absoluteTableIdentifier, LockUsage.CLEAN_FILES_LOCK, errorMsg)
        if (truncateTable) {
          SegmentStatusManager.truncateTable(carbonTable)
        }
        SegmentStatusManager.deleteLoadsAndUpdateMetadata(
          carbonTable, true, currentTablePartitions.map(_.asJava).orNull)
        CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, true)
        currentTablePartitions match {
          case Some(partitions) =>
            SegmentFileStore.cleanSegments(
              carbonTable,
              currentTablePartitions.map(_.asJava).orNull,
              true)
          case _ =>
        }
      }
    } finally {
      if (currentTablePartitions.equals(None)) {
        cleanUpPartitionFoldersRecursively(carbonTable, List.empty[PartitionSpec])
      } else {
        cleanUpPartitionFoldersRecursively(carbonTable, currentTablePartitions.get.toList)
      }

      if (carbonCleanFilesLock != null) {
        CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
      }
    }
  }


  /**
   * delete partition folders recursively
   *
   * @param carbonTable
   * @param partitionSpecList
   */
  def cleanUpPartitionFoldersRecursively(carbonTable: CarbonTable,
      partitionSpecList: List[PartitionSpec]): Unit = {
    if (carbonTable != null && carbonTable.isHivePartitionTable) {
      val loadMetadataDetails = SegmentStatusManager
        .readLoadMetadata(carbonTable.getMetadataPath)

      val carbonFile = FileFactory.getCarbonFile(carbonTable.getTablePath)

      // list all files from table path
      val listOfDefaultPartFilesIterator = carbonFile.listFiles(true)
      loadMetadataDetails.foreach { metadataDetail =>
        if (metadataDetail.getSegmentStatus.equals(SegmentStatus.MARKED_FOR_DELETE) &&
          metadataDetail.getSegmentFile == null) {
          val loadStartTime: Long = metadataDetail.getLoadStartTime
          // delete all files of @loadStartTime from table path
          cleanCarbonFilesInFolder(listOfDefaultPartFilesIterator, loadStartTime)
          partitionSpecList.foreach {
            partitionSpec =>
              val partitionLocation = partitionSpec.getLocation
              // For partition folder outside the tablePath
              if (!partitionLocation.toString.startsWith(carbonTable.getTablePath)) {
                val partitionCarbonFile = FileFactory
                  .getCarbonFile(partitionLocation.toString)
                // list all files from partitionLocation
                val listOfExternalPartFilesIterator = partitionCarbonFile.listFiles(true)
                // delete all files of @loadStartTime from externalPath
                cleanCarbonFilesInFolder(listOfExternalPartFilesIterator, loadStartTime)
              }
          }
        }
      }
    }
  }

  /**
   * Compare CarbonFile Timestamp and delete files.
   *
   * @param carbonFiles
   * @param timestamp
   */
  private def cleanCarbonFilesInFolder(carbonFiles: java.util.List[CarbonFile],
      timestamp: Long): Unit = {
    carbonFiles.asScala.foreach { carbonFile =>
        val filePath = carbonFile.getPath
        val fileName = carbonFile.getName
        if (CarbonTablePath.DataFileUtil.compareCarbonFileTimeStamp(fileName, timestamp)) {
          FileFactory.deleteFile(filePath)
        }
    }
  }

  /**
   * The in-progress segments which are in stale state will be marked as deleted
   * when driver is initializing.
   *
   * @param databaseLocation
   * @param dbName
   */
  def cleanInProgressSegments(databaseLocation: String, dbName: String): Unit = {
    val loaderDriver = CarbonProperties.getInstance().
      getProperty(CarbonCommonConstants.DATA_MANAGEMENT_DRIVER,
        CarbonCommonConstants.DATA_MANAGEMENT_DRIVER_DEFAULT).toBoolean
    if (!loaderDriver) {
      return
    }
    try {
      if (FileFactory.isFileExist(databaseLocation)) {
        val file = FileFactory.getCarbonFile(databaseLocation)
        if (file.isDirectory) {
          val tableFolders = file.listFiles()
          tableFolders.foreach { tableFolder =>
            if (tableFolder.isDirectory) {
              val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR +
               tableFolder.getName
              val tableUniqueName = CarbonTable.buildUniqueName(dbName, tableFolder.getName)
              val tableStatusFile =
                CarbonTablePath.getTableStatusFilePath(tablePath)
              if (FileFactory.isFileExist(tableStatusFile)) {
                try {
                  val carbonTable = CarbonMetadata.getInstance.getCarbonTable(tableUniqueName)
                  SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, true, null)
                } catch {
                  case _: Exception =>
                    LOGGER.warn(s"Error while cleaning table " + s"$tableUniqueName")
                }
              }
            }
          }
        }
      }
    } catch {
      case s: java.io.FileNotFoundException =>
        LOGGER.error(s)
    }
  }

  /**
   * The below method deletes all the files and folders in the trash folders of all carbon tables
   * in all databases
   */
  def deleteDataFromTrashFolderInAllTables(sparkSession: SparkSession): Unit = {
    try {
      val databases = sparkSession.sessionState.catalog.listDatabases()
      databases.foreach(dbName => {
        val databaseLocation = CarbonEnv.getDatabaseLocation(dbName, sparkSession)
        if (FileFactory.isFileExist(databaseLocation)) {
          val file = FileFactory.getCarbonFile(databaseLocation)
          if (file.isDirectory) {
            val tableFolders = file.listFiles()
            tableFolders.foreach { tableFolder =>
              if (tableFolder.isDirectory) {
                val tablePath = databaseLocation +
                  CarbonCommonConstants.FILE_SEPARATOR + tableFolder.getName
                TrashUtil.deleteAllDataFromTrashFolder(tablePath)
              }
            }
          }
        }
      })
    } catch {
      case e: Throwable =>
        // catch all exceptions to avoid failure
        LOGGER.error("Failed to clear trash folder of all tables", e)
    }
  }
  /**
   * The below method deletes all the files and folders in trash folder in a carbontable and all
   * it's index tables
   */
  def deleteDataFromTrashFolder(carbonTable: CarbonTable, sparkSession: SparkSession): Unit = {
    TrashUtil.deleteAllDataFromTrashFolder(carbonTable.getTablePath)
    // check for index tables
    val indexTables = CarbonIndexUtil
      .getIndexCarbonTables(carbonTable, sparkSession)
    indexTables.foreach { indexTable =>
      TrashUtil.deleteAllDataFromTrashFolder(indexTable.getTablePath)
    }
  }

  /**
   * The below method deletes the timestamp sub directory in the trash folder based on the
   * expiration day
   */
  def deleteStaleDataFromTrash(carbonTable: CarbonTable, sparkSession:
  SparkSession): Unit = {
    val expirationDay = CarbonProperties.getInstance().getTrashFolderExpirationTime
    TrashUtil.deleteAllDataFromTrashFolderByTimeStamp(carbonTable.getTablePath, expirationDay)
    // check for index tables
    val indexTables = CarbonIndexUtil
      .getIndexCarbonTables(carbonTable, sparkSession)
    indexTables.foreach { indexTable =>
      TrashUtil.deleteAllDataFromTrashFolderByTimeStamp(indexTable.getTablePath, expirationDay)
    }
  }

  /**
   * Actual dry run operation. It will also do dry run for stale segments
   */
  def dryRunCleanFiles(carbonTable: CarbonTable, sparkSession: SparkSession): Seq[Row] = {
    // dry run for clean files command
    // Clean files will remove compacted, Marked_for_delete, Insert in progress(stale) segments.
    val tableStatusLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
    var loadMetadataDetails = List[LoadMetadataDetails]()
    try {
      if (tableStatusLock.lockWithRetries()) {
        val tableStatusFilePath = CarbonTablePath
          .getTableStatusFilePath(carbonTable.getTablePath)
        loadMetadataDetails = SegmentStatusManager.readTableStatusFile(tableStatusFilePath).toList
      } else {
        throw new ConcurrentOperationException(carbonTable.getDatabaseName,
          carbonTable.getTableName, "table status read", "clean files command dry run")
      }
    } finally {
      tableStatusLock.unlock()
    }

    // no need to show the first and the last segments here if they are already deleted but their
    // entry is in the tablestatus
    val finalsSegments = new ListBuffer[Row]
    loadMetadataDetails.map(loadDetails =>
      if ((loadDetails.getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE || loadDetails
        .getSegmentStatus == SegmentStatus.COMPACTED)) {
        if (loadDetails.getVisibility == null || (loadDetails.getVisibility != null && loadDetails
          .getVisibility.toBoolean)) {
          finalsSegments += Row(carbonTable.getTableName, loadDetails.getLoadName,
            loadDetails.getSegmentStatus.toString, "FACT", "DELETE", "NULL")
        }
      } else if (loadDetails.getSegmentStatus == SegmentStatus.INSERT_IN_PROGRESS && (loadDetails
        .getVisibility == null || (loadDetails.getVisibility != null && loadDetails.getVisibility
        .toBoolean))) {
        // try getting segment lock, if the lock is available, then it is a stale segment
        // and can be moved to trash
        if (CarbonUtil.tryGettingSegmentLock(loadDetails, carbonTable.getAbsoluteTableIdentifier)) {
          finalsSegments += Row(carbonTable.getTableName, loadDetails.getLoadName,
            loadDetails.getSegmentStatus.toString, "FACT", "DELETE", "NULL")
        }
      })
    finalsSegments ++= dryRunStaleSegments(carbonTable, sparkSession)
    finalsSegments
  }

  /**
   * Dry run operation for carbonTable and all it's index tables
   */
  def cleanFilesDryRun(carbonTable: CarbonTable, sparkSession: SparkSession): Seq[Row] = {
    var res = dryRunCleanFiles(carbonTable, sparkSession)
    res ++= trashFolderDryRun(carbonTable)
    // check for index tables
    val indexTables = CarbonIndexUtil
      .getIndexCarbonTables(carbonTable, sparkSession)
    indexTables.foreach { indexTable =>
        res ++= dryRunCleanFiles(indexTable, sparkSession)
        res ++= trashFolderDryRun(indexTable)
    }
  res
  }


  /**
   * Dry run operation for stale segments
   */
  def dryRunStaleSegments(carbonTable: CarbonTable, sparkSession: SparkSession): Seq[Row] = {
    // stale segments are those segments whose entry is not present in the table status file, but
    // those segments are in Fact folder and metadata folder

    val metaDataLocation = carbonTable.getMetadataPath
    val details = SegmentStatusManager.readLoadMetadata(metaDataLocation)
    val finalSegments = new java.util.ArrayList[Row]()

    val partitionPath = CarbonTablePath.getPartitionDir(carbonTable.getTablePath)
    if (FileFactory.isFileExist(partitionPath)) {
      val allSegments = FileFactory.getCarbonFile(partitionPath).listFiles
      // there is no segment
      if (allSegments == null || allSegments.isEmpty) Seq.empty
      // there is no segment or failed to read tablestatus file.
      if (details == null || details.isEmpty) Seq.empty
      val staleSegments = TableProcessingOperations.getStaleSegments(details, allSegments, false)
      staleSegments.asScala.foreach(
        staleSegmentName => finalSegments.add(Row(carbonTable.getTableName, staleSegmentName._2,
          "UNKNOWN", "FACT", "MOVE TO TRASH"))
      )
    } else {
      // for partition table flow
      val partitionFolderList : java.util.ArrayList[String] = new util.ArrayList()
      val allFolders = FileFactory.getCarbonFile(carbonTable.getTablePath).listFiles()
      allFolders.toSeq.foreach( name =>
        if (name.getName.contains(CarbonCommonConstants.EQUALS) ) {
          partitionFolderList.add(name.getAbsolutePath)
        })
      val tableLoadDetails = details.map(detail => detail.getLoadName)
      partitionFolderList.asScala.foreach(name =>
        FileFactory.getCarbonFile(name).listFiles().foreach(fileName =>
          if (fileName.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT) && !tableLoadDetails
            .contains(fileName.getName.substring(0, fileName.getName.indexOf(CarbonCommonConstants
              .UNDERSCORE)))) {
            val filePath = fileName.getAbsolutePath.split(CarbonCommonConstants.FILE_SEPARATOR)
            finalSegments.add(Row(carbonTable.getTableName, fileName.getName.substring(0, fileName
              .getName.indexOf("_")), "UNKNOWN", filePath(filePath.length - 2), "MOVE TO TRASH"))
          }
        )
      )
    }
    finalSegments.asScala
  }

  /**
   * Dry run operation for trash folder
   */
  def trashFolderDryRun(carbonTable: CarbonTable): Seq[Row] = {
    val finalSegments = new java.util.ArrayList[Row]()
    val listSegments = TrashUtil.listSegmentsInTrashFolder(carbonTable.getTablePath)
    listSegments.asScala.foreach(
      listElement => finalSegments.add(Row(carbonTable.getTableName, listElement.substring(
        listElement.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1), "UNKNOWN",
        CarbonTablePath.CARBON_TRASH_FOLDER_NAME + CarbonCommonConstants.FILE_SEPARATOR +
          listElement.substring(1), "DELETE FROM TRASH", timeLeftToExpiration(listElement
          .split("/")(1))))
    )
    finalSegments.asScala
  }

  def timeLeftToExpiration(timeStamp : String): String = {
    val expirationTime = CarbonProperties.getInstance().getTrashFolderExpirationTime
    val currentTime = String.valueOf(new Timestamp(System.currentTimeMillis).getTime)
    val timeSinceCreation = currentTime.toLong - timeStamp.toLong
    if (timeSinceCreation > expirationTime || expirationTime == 0) {
      // folder has expired already
      "EXPIRED"
    } else {
      // time left is expirationTime - timeSinceCreation
      ((expirationTime - timeSinceCreation)*1.0/ TimeUnit.DAYS.toMillis(1)).toString
    }
  }
}
