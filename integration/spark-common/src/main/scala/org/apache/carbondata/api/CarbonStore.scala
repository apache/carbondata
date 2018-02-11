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
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.spark.util.DataLoadingUtil

object CarbonStore {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def showSegments(
      limit: Option[String],
      tableFolderPath: String): Seq[Row] = {
    val loadMetadataDetailsArray = SegmentStatusManager.readLoadMetadata(tableFolderPath)
    if (loadMetadataDetailsArray.nonEmpty) {
      var loadMetadataDetailsSortedArray = loadMetadataDetailsArray.sortWith { (l1, l2) =>
        java.lang.Double.parseDouble(l1.getLoadName) > java.lang.Double.parseDouble(l2.getLoadName)
      }
      if (limit.isDefined) {
        loadMetadataDetailsSortedArray = loadMetadataDetailsSortedArray
          .filter(load => load.getVisibility.equalsIgnoreCase("true"))
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
        .filter(_.getVisibility.equalsIgnoreCase("true"))
        .map { load =>
          val mergedTo =
            if (load.getMergedLoadName != null) {
              load.getMergedLoadName
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

          Row(
            load.getLoadName,
            load.getSegmentStatus.getMessage,
            startTime,
            endTime,
            mergedTo,
            load.getFileFormat.toString)
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
      currentTablePartitions: Option[Seq[PartitionSpec]] = None): Unit = {
    LOGGER.audit(s"The clean files request has been received for $dbName.$tableName")
    var carbonCleanFilesLock: ICarbonLock = null
    val absoluteTableIdentifier = if (forceTableClean) {
      AbsoluteTableIdentifier.from(tablePath, dbName, tableName)
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
        SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, true)
        DataLoadingUtil.deleteLoadsAndUpdateMetadata(
          isForceDeletion = true, carbonTable, currentTablePartitions.map(_.asJava).orNull)
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
      if (carbonCleanFilesLock != null) {
        CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK)
      }
    }
    LOGGER.audit(s"Clean files operation is success for $dbName.$tableName.")
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
