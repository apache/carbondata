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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{SegmentManager, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath

object CarbonStore {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def showSegments(
      limit: Option[String],
      identifier: AbsoluteTableIdentifier,
      showHistory: Boolean): Seq[Row] = {
    val loadMetadataDetailsArray = if (showHistory) {
      new SegmentManager().getAllSegments(identifier).getAllSegments.asScala ++
      new SegmentManager().getAllHistorySegments(identifier).getAllSegments.asScala
    } else {
      new SegmentManager().getAllSegments(identifier).getAllSegments.asScala
    }

    if (loadMetadataDetailsArray.nonEmpty) {
      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith { (l1, l2) =>
        l1.getLoadStartTime > l2.getLoadStartTime
      }
      if (!showHistory) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray
          .filter(_.getVisibility)
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
            if (load.getMergedSegmentIds != null) {
              load.getMergedSegmentIds
            } else {
              "NA"
            }

          val startTime =
            if (load.getLoadStartTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
              null
            } else {
              new java.sql.Timestamp(load.getLoadStartTime)
            }

          val endTime =
            if (load.getLoadEndTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
              null
            } else {
              new java.sql.Timestamp(load.getLoadEndTime)
            }

          if (showHistory) {
            Row(
              load.getSegmentId,
              load.getStatus,
              startTime,
              endTime,
              mergedTo,
              load.getFileFormat,
              load.getVisibility)
          } else {
            Row(
              load.getSegmentId,
              load.getStatus,
              startTime,
              endTime,
              mergedTo,
              load.getFileFormat)
          }
        }
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
      currentTablePartitions: Option[Seq[PartitionSpec]] = None): Unit = {
    LOGGER.audit(s"The clean files request has been received for $dbName.$tableName")
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
          FileFactory.getCarbonFile(absoluteTableIdentifier.getTablePath,
            FileFactory.getFileType(absoluteTableIdentifier.getTablePath)))
      } else {
        carbonCleanFilesLock =
          CarbonLockUtil
            .getLockObject(absoluteTableIdentifier, LockUsage.CLEAN_FILES_LOCK, errorMsg)
        new SegmentManager().deleteLoadsAndUpdateMetadata(
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
        cleanUpPartitionFoldersRecurssively(carbonTable, List.empty[PartitionSpec])
      } else {
        cleanUpPartitionFoldersRecurssively(carbonTable, currentTablePartitions.get.toList)
      }

      if (carbonCleanFilesLock != null) {
        CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
      }
    }
    LOGGER.audit(s"Clean files operation is success for $dbName.$tableName.")
  }

  /**
   * delete partition folders recurssively
   *
   * @param carbonTable
   * @param partitionSpecList
   */
  def cleanUpPartitionFoldersRecurssively(carbonTable: CarbonTable,
      partitionSpecList: List[PartitionSpec]): Unit = {
    if (carbonTable != null) {
      val loadMetadataDetails =
        new SegmentManager().getInvalidSegments(
          carbonTable.getAbsoluteTableIdentifier).getInValidSegmentDetailVOs.asScala

      val fileType = FileFactory.getFileType(carbonTable.getTablePath)
      val carbonFile = FileFactory.getCarbonFile(carbonTable.getTablePath, fileType)

      // list all files from table path
      val listOfDefaultPartFilesIterator = carbonFile.listFiles(true)
      loadMetadataDetails.foreach { metadataDetail =>
        if (metadataDetail.getStatus.equals(SegmentStatus.MARKED_FOR_DELETE.toString) &&
            metadataDetail.getSegmentFileName == null) {
          val loadStartTime: Long = metadataDetail.getLoadStartTime
          // delete all files of @loadStartTime from tablepath
          cleanCarbonFilesInFolder(listOfDefaultPartFilesIterator, loadStartTime)
          partitionSpecList.foreach {
            partitionSpec =>
              val partitionLocation = partitionSpec.getLocation
              // For partition folder outside the tablePath
              if (!partitionLocation.toString.startsWith(carbonTable.getTablePath)) {
                val fileType = FileFactory.getFileType(partitionLocation.toString)
                val partitionCarbonFile = FileFactory
                  .getCarbonFile(partitionLocation.toString, fileType)
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
          FileFactory.deleteFile(filePath, FileFactory.getFileType(filePath))

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

    LOGGER.audit(s"Delete segment by Id request has been received for $dbName.$tableName")
    validateLoadIds(loadids)

    val path = carbonTable.getMetadataPath

    try {
      val invalidLoadIds = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadids.asJava, path).asScala
      if (invalidLoadIds.isEmpty) {
        LOGGER.audit(s"Delete segment by Id is successfull for $dbName.$tableName.")
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
    LOGGER.audit(s"Delete segment by Id request has been received for $dbName.$tableName")

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
        LOGGER.audit(s"Delete segment by date is successful for $dbName.$tableName.")
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
    val identifier = AbsoluteTableIdentifier.from(storePath, dbName, tableName)
    val validSegments = new
        SegmentManager().getValidSegments(identifier).getValidSegments
    validSegments.contains(Segment.toSegment(segmentId))
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
