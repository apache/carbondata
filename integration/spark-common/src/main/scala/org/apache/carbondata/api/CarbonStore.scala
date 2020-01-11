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

package org.apache.carbondata.api

import java.lang.Long

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.segment.StreamSegment

object CarbonStore {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def showSegments(
      limit: Option[String],
      tablePath: String,
      showHistory: Boolean): Seq[Row] = {
    val metaFolder = CarbonTablePath.getMetadataPath(tablePath)
    val loadMetadataDetailsArray = if (showHistory) {
      SegmentStatusManager.readLoadMetadata(metaFolder) ++
      SegmentStatusManager.readLoadHistoryMetadata(metaFolder)
    } else {
      SegmentStatusManager.readLoadMetadata(metaFolder)
    }

    if (loadMetadataDetailsArray.nonEmpty) {
      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith { (l1, l2) =>
        java.lang.Double.parseDouble(l1.getLoadName) > java.lang.Double.parseDouble(l2.getLoadName)
      }
      if (!showHistory) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray
          .filter(_.getVisibility.equalsIgnoreCase("true"))
      }
      if (limit.isDefined) {
        val limitLoads = limit.get
        try {
          val lim = Integer.parseInt(limitLoads)
          loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray.slice(0, lim)
        } catch {
          case _: NumberFormatException =>
            CarbonException.analysisException("Entered limit is not a valid Number")
        }
      }

      loadMetadataDetailsSortedArray
        .map { load =>
          val mergedTo =
            if (load.getMergedLoadName != null) {
              load.getMergedLoadName
            } else {
              "NA"
            }

          val path =
            if (StringUtils.isNotEmpty(load.getPath)) {
              load.getPath
            } else {
              "NA"
            }

          val startTime =
            if (load.getLoadStartTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
              "NA"
            } else {
              new java.sql.Timestamp(load.getLoadStartTime).toString
            }

          val endTime =
            if (load.getLoadEndTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
              "NA"
            } else {
              new java.sql.Timestamp(load.getLoadEndTime).toString
            }

          val (dataSize, indexSize) = if (load.getFileFormat.equals(FileFormat.ROW_V1)) {
            // for streaming segment, we should get the actual size from the index file
            // since it is continuously inserting data
            val segmentDir = CarbonTablePath.getSegmentPath(tablePath, load.getLoadName)
            val indexPath = CarbonTablePath.getCarbonStreamIndexFilePath(segmentDir)
            val indexFile = FileFactory.getCarbonFile(indexPath)
            if (indexFile.exists()) {
              val indices =
                StreamSegment.readIndexFile(indexPath)
              (indices.asScala.map(_.getFile_size).sum, indexFile.getSize)
            } else {
              (-1L, -1L)
            }
          } else {
            // If the added segment is other than carbon segment then we can only display the data
            // size and not index size, we can get the data size from table status file directly
            if (!load.getFileFormat.isCarbonFormat) {
              (if (load.getIndexSize == null) -1L else load.getIndexSize.toLong, -1L)
            } else {
              (if (load.getDataSize == null) -1L else load.getDataSize.toLong,
                if (load.getIndexSize == null) -1L else load.getIndexSize.toLong)
            }
          }

          if (showHistory) {
            Row(
              load.getLoadName,
              load.getSegmentStatus.getMessage,
              startTime,
              endTime,
              mergedTo,
              load.getFileFormat.toString.toUpperCase,
              load.getVisibility,
              Strings.formatSize(dataSize.toFloat),
              Strings.formatSize(indexSize.toFloat),
              path)
          } else {
            Row(
              load.getLoadName,
              load.getSegmentStatus.getMessage,
              startTime,
              endTime,
              mergedTo,
              load.getFileFormat.toString.toUpperCase,
              Strings.formatSize(dataSize.toFloat),
              Strings.formatSize(indexSize.toFloat),
              path)
          }
        }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * The method deletes all data if forceTableCLean <true> and lean garbage segment
   * (MARKED_FOR_DELETE state) if forceTableCLean <false>
   *
   * @param dbName          : Database name
   * @param tableName       : Table name
   * @param tablePath       : Table path
   * @param carbonTable     : CarbonTable Object <null> in case of force clean
   * @param forceTableClean : <true> for force clean it will delete all data
   *                        <false> it will clean garbage segment (MARKED_FOR_DELETE state)
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
          // delete all files of @loadStartTime from tablepath
          cleanCarbonFilesInFolder(listOfDefaultPartFilesIterator, loadStartTime)
          partitionSpecList.foreach {
            partitionSpec =>
              val partitionLocation = partitionSpec.getLocation
              // For partition folder outside the tablePath
              if (!partitionLocation.toString.startsWith(carbonTable.getTablePath)) {
                val partitionCarbonFile = FileFactory
                  .getCarbonFile(partitionLocation.toString)
                // list all files from partitionLoacation
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
   *
   * @param carbonFiles
   * @param timestamp
   */
  private def cleanCarbonFilesInFolder(carbonFiles: java.util.List[CarbonFile],
      timestamp: Long): Unit = {
    carbonFiles.asScala.foreach {
      carbonFile =>
        val filePath = carbonFile.getPath
        val fileName = carbonFile.getName
        if (CarbonTablePath.DataFileUtil.compareCarbonFileTimeStamp(fileName, timestamp)) {
          // delete the file
          FileFactory.deleteFile(filePath)
        }
    }
  }

  // validates load ids
  private def validateLoadIds(loadids: Seq[String]): Unit = {
    if (loadids.isEmpty) {
      val errorMessage = "Error: Segment id(s) should not be empty."
      throw new MalformedCarbonCommandException(errorMessage)
    }
  }

  // TODO: move dbName and tableName to caller, caller should handle the log and error
  def deleteLoadById(
      loadids: Seq[String],
      dbName: String,
      tableName: String,
      carbonTable: CarbonTable): Unit = {

    validateLoadIds(loadids)

    val path = carbonTable.getMetadataPath

    try {
      val invalidLoadIds = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadids.asJava, path).asScala
      if (invalidLoadIds.isEmpty) {
        LOGGER.info(s"Delete segment by Id is successfull for $dbName.$tableName.")
      } else {
        sys.error(s"Delete segment by Id is failed. Invalid ID is: ${invalidLoadIds.mkString(",")}")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
    Seq.empty
  }

  // TODO: move dbName and tableName to caller, caller should handle the log and error
  def deleteLoadByDate(
      timestamp: String,
      dbName: String,
      tableName: String,
      carbonTable: CarbonTable): Unit = {

    val time = validateTimeFormat(timestamp)
    val path = carbonTable.getMetadataPath

    try {
      val invalidLoadTimestamps =
        SegmentStatusManager.updateDeletionStatus(
          carbonTable.getAbsoluteTableIdentifier,
          timestamp,
          path,
          time).asScala
      if (invalidLoadTimestamps.isEmpty) {
        LOGGER.info(s"Delete segment by date is successful for $dbName.$tableName.")
      } else {
        sys.error("Delete segment by date is failed. No matching segment found.")
      }
    } catch {
      case ex: Exception =>
        sys.error(ex.getMessage)
    }
  }

  // this function is for test only
  def isSegmentValid(
      dbName: String,
      tableName: String,
      storePath: String,
      segmentId: String): Boolean = {
    val identifier = AbsoluteTableIdentifier.from(storePath, dbName, tableName, tableName)
    val validAndInvalidSegments: SegmentStatusManager.ValidAndInvalidSegmentsInfo = new
        SegmentStatusManager(
          identifier).getValidAndInvalidSegments
    validAndInvalidSegments.getValidSegments.contains(segmentId)
  }

  private def validateTimeFormat(timestamp: String): Long = {
    try {
      DateTimeUtils.stringToTimestamp(UTF8String.fromString(timestamp)).get
    } catch {
      case e: Exception =>
        val errorMessage = "Error: Invalid load start time format: " + timestamp
        throw new MalformedCarbonCommandException(errorMessage)
    }
  }

}
