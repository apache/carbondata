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

package org.apache.carbondata.recovery.tablestatus

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.mutate.SegmentUpdateDetails
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.util.CarbonScalaUtil

object TableStatusRecovery {
  def main(args: Array[String]): Unit = {
    // check the argument contains database name and tablename to recover table status file
    assert(args.length == 2)
    createCarbonSession()
    val sparkSession = SparkSQLUtil.getSparkSession
    val tableName = args(1)
    val databaseName = args(0)
    // get carbon table to start table status recovery
    val carbonTable = try {
      CarbonEnv.getCarbonTable(Some(databaseName), tableName)(sparkSession)
    } catch {
      case ex: Exception =>
        throw ex
    }

    if (carbonTable.isMV) {
      // not supported
      throw new UnsupportedOperationException("Unsupported operation on Materialized view table")
    }

    /**
     * 1. get the current table status version file name associated with carbon table
     * 2. Check if the current table status version file exists
     * 3. If does not exists, then read all the old table status version files and find the last
     *    recent version file and get the load metadata details. For the lost load metadata,
     *    read the segment files and table status update files to recover the lost
     *    load metadata entry and add it to previous version load metadata details list.
     * 4. Write the load metadata details list with version name as [Step:1]
     * */
    val tableStatusVersion = carbonTable.getTableStatusVersion
    val tableStatusPath = CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath,
      tableStatusVersion)
    val tableStatusFile = FileFactory.getCarbonFile(
      FileFactory.getUpdatedFilePath(tableStatusPath))
    if (!tableStatusFile.exists()) {
      // case where the current version table status file is lost, then get the previous table
      // status version file and update it as the current table status version
      val tableStatusFiles = CarbonScalaUtil.getTableStatusVersionFiles(carbonTable.getTablePath)
      // read the segment files in the Metadata directory
      val segmentFileDir = FileFactory.getCarbonFile(FileFactory.getUpdatedFilePath(
        CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath)))
      val segmentFiles = segmentFileDir.listFiles()
        .filter(segmentFile => segmentFile.getName.endsWith(CarbonTablePath.SEGMENT_EXT))
        .toList
      if (tableStatusFiles.isEmpty) {
        if (segmentFiles.isEmpty) {
          // no metadata found to recover table status file
          throw new Exception(
            "Segment Files does not exists to recover load metadata")
        }
      }
      // prepare segment to latest timestamp version map. This is required, in case of drop
      // partition, where there can be multiple segment files for same segment Id
      val segToTimeStampMap = new util.HashMap[String, String]()
      segmentFiles.foreach { segmentFile =>
        val segFileName = segmentFile.getName
        val segmentToTimestamp = segFileName.trim.split(CarbonCommonConstants.UNDERSCORE).toList
        if (!segToTimeStampMap.containsKey(segmentToTimestamp.head)) {
          segToTimeStampMap.put(segmentToTimestamp.head, segmentToTimestamp.last)
        } else {
          val timeStamp = segToTimeStampMap.get(segmentToTimestamp.head)
          if (timeStamp <= segmentToTimestamp.last) {
            segToTimeStampMap.put(segmentToTimestamp.head, segmentToTimestamp.last)
          }
        }
      }
      // iterate the available table status version files and find the most recent table status
      // version file
      val latestTableStatusVersionStr = CarbonScalaUtil.getLatestTblStatusVersionBasedOnTimestamp(
        tableStatusFiles)

      // read the load metadata details with the identified table status version file
      var loadMetaDetails = SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(
        carbonTable.getTablePath), latestTableStatusVersionStr).toList

      var updateMetaDetails: Array[SegmentUpdateDetails] = Array.empty

      val tableUpdateStatusFiles = FileFactory.getCarbonFile(CarbonTablePath.getMetadataPath(
        carbonTable.getTablePath)).listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = {
          file.getName.startsWith(CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME)
        }
      })

      // if table has table update status files, iterate and identify the latest table status
      // update file
      if (tableUpdateStatusFiles.nonEmpty) {
        var latestTableUpdateStatusVersion = 0L
        tableUpdateStatusFiles.foreach { tableStatusFile =>
          val updateVersionTimeStamp = tableStatusFile.getName
            .substring(tableStatusFile.getName.indexOf(CarbonCommonConstants.HYPHEN) + 1,
              tableStatusFile.getName.length).toLong
          if (latestTableUpdateStatusVersion <= updateVersionTimeStamp) {
            latestTableUpdateStatusVersion = updateVersionTimeStamp
          }
        }
        updateMetaDetails = SegmentUpdateStatusManager.readLoadMetadata(
          CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME + CarbonCommonConstants.HYPHEN +
          latestTableUpdateStatusVersion.toString, carbonTable.getTablePath)
      }

      // check which segment is missing from lost table status version
      val missedLoadMetaDetails: util.List[LoadMetadataDetails] =
        new util.ArrayList[LoadMetadataDetails]()
      segToTimeStampMap.asScala.foreach { segmentFileEntry =>
        val segmentFileName = segmentFileEntry._1 + CarbonCommonConstants.UNDERSCORE +
                              segmentFileEntry._2
        val segmentId = segmentFileEntry._1
        val segmentUpdateDetail = updateMetaDetails
          .filter(_.getSegmentName.equalsIgnoreCase(segmentId))
        // check if the segment Id from segment file entry exists in load metadata details list.
        // If does not exist, or if the segment file mapped to the load metadata entry and the
        // latest segment file timestamp is not same, then prepare new load metadata.
        if ((!loadMetaDetails.exists(_.getLoadName.equalsIgnoreCase(segmentId))
             || !loadMetaDetails.filter(_.getLoadName.equalsIgnoreCase(segmentId))
          .head.getSegmentFile.equalsIgnoreCase(segmentFileName)) &&
            !segmentId.contains(CarbonCommonConstants.POINT)) {
          val segFilePath = CarbonTablePath.getSegmentFilePath(
            carbonTable.getTablePath, segmentFileName)
          // read segment file and prepare load metadata
          val segmentFile = SegmentFileStore.readSegmentFile(segFilePath)
          val loadMetadataDetail = new LoadMetadataDetails()
          val segmentInfo = segmentFile.getLocationMap.asScala.head._2
          if (!segmentUpdateDetail.isEmpty) {
            loadMetadataDetail.setSegmentStatus(segmentUpdateDetail.head.getSegmentStatus)
            loadMetadataDetail.setModificationOrDeletionTimestamp(segmentUpdateDetail.head
              .getDeleteDeltaStartTimeAsLong)
          } else {
            loadMetadataDetail.setSegmentStatus(getSegmentStatus(segmentInfo.getStatus))
          }
          loadMetadataDetail.setLoadName(segmentId)
          loadMetadataDetail.setSegmentFile(segmentFileName)
          val dataIndexSize = CarbonUtil.getDataSizeAndIndexSize(carbonTable
            .getTablePath, new Segment(segmentId, segmentFileName))
          loadMetadataDetail.setDataSize(dataIndexSize
            .get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toString)
          loadMetadataDetail.setIndexSize(dataIndexSize
            .get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toString)
          loadMetadataDetail.setLoadEndTime(FileFactory
            .getCarbonFile(segFilePath)
            .getLastModifiedTime)
          missedLoadMetaDetails.add(loadMetadataDetail)
          if (loadMetaDetails.exists(_.getLoadName.equalsIgnoreCase(segmentId))) {
            loadMetaDetails = loadMetaDetails.filterNot(_.getLoadName
              .equalsIgnoreCase(segmentId))
          }
        } else if (!segmentUpdateDetail.isEmpty) {
          // in case of Update/delete, update the already existing load metadata entry with the
          // latest segment update detail
          val loadMetadataDetail = loadMetaDetails
            .find(_.getLoadName.equalsIgnoreCase(segmentId))
            .head
          loadMetadataDetail.setSegmentStatus(segmentUpdateDetail.head.getSegmentStatus)
          loadMetadataDetail.setModificationOrDeletionTimestamp(segmentUpdateDetail.head
            .getDeleteDeltaStartTimeAsLong)
          loadMetaDetails = loadMetaDetails.filterNot(_.getLoadName.equalsIgnoreCase(segmentId))
          missedLoadMetaDetails.add(loadMetadataDetail)
        }
      }
      missedLoadMetaDetails.addAll(loadMetaDetails.asJava)
      // write new table status file with lost table status version name
      SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
        carbonTable.getTablePath, tableStatusVersion),
        missedLoadMetaDetails.toArray(new Array[LoadMetadataDetails](missedLoadMetaDetails
          .size)))
    }
  }

  private def getSegmentStatus(segmentStatus: String): SegmentStatus = {
    if (segmentStatus.equalsIgnoreCase("success")) {
      SegmentStatus.SUCCESS
    } else if (segmentStatus.equalsIgnoreCase("Marked for Delete")) {
      SegmentStatus.MARKED_FOR_DELETE
    } else if (segmentStatus.equalsIgnoreCase("Failure")) {
      SegmentStatus.LOAD_FAILURE
    } else {
      SegmentStatus.COMPACTED
    }
  }

  private def createCarbonSession(): SparkSession = {
    val spark = SparkSession
      .builder().config(new SparkConf())
      .appName("RecoveryTool")
      .enableHiveSupport()
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()
    CarbonEnv.getInstance(spark)

    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    spark
  }
}
