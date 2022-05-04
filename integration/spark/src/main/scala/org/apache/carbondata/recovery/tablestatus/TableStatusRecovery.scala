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

object TableStatusRecovery {
  def main(args: Array[String]): Unit = {
    assert(args.length == 2)
    createCarbonSession()
    val sparkSession = SparkSQLUtil.getSparkSession
    val tableName = args(1)
    val databaseName = args(0)
    val carbonTable = try {
      CarbonEnv.getCarbonTable(Some(databaseName), tableName)(sparkSession)
    } catch {
      case ex: Exception =>
        throw ex
    }
    val tableStatusVersion = carbonTable.getTableStatusVersion
    if (tableStatusVersion.nonEmpty) {
      val tableStatusPath = CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath,
        tableStatusVersion)
      val tableStatusFile = FileFactory.getCarbonFile(FileFactory.getUpdatedFilePath
      (tableStatusPath))
      if (!tableStatusFile.exists()) {
        // case where the current version table status file is lost, then get the previous table
        // status version file and update it as the current table status version
        val tableStatusFiles = FileFactory.getCarbonFile(CarbonTablePath.getMetadataPath(carbonTable
          .getTablePath)).listFiles(new CarbonFileFilter {
          override def accept(file: CarbonFile): Boolean = {
            file.getName.startsWith(CarbonTablePath
              .TABLE_STATUS_FILE)
          }
        })
        if (tableStatusFiles.isEmpty) {
          throw new Exception("Table Status Version Files are missing")
        }
        var latestTableStatusVersion = 0L
        tableStatusFiles.foreach { tableStatusFile =>
          val versionTimeStamp = tableStatusFile.getName
            .substring(tableStatusFile.getName.indexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              tableStatusFile.getName.length).toLong
          if (latestTableStatusVersion <= versionTimeStamp) {
            latestTableStatusVersion = versionTimeStamp
          }
        }
        var loadMetaDetails = SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(
          carbonTable.getTablePath), latestTableStatusVersion.toString).toList

        var updateMetaDetails: Array[SegmentUpdateDetails] = Array.empty

        val tableUpdateStatusFiles = FileFactory.getCarbonFile(CarbonTablePath.getMetadataPath(
          carbonTable.getTablePath)).listFiles(new CarbonFileFilter {
          override def accept(file: CarbonFile): Boolean = {
            file.getName.startsWith(CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME)
          }
        })

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
          val segmentFileDir = FileFactory.getCarbonFile(FileFactory.getUpdatedFilePath(
            CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath)))
          val segmentFiles = segmentFileDir.listFiles()
            .map(_.getName)
            .filter(segmentFileName => segmentFileName.endsWith(CarbonTablePath.SEGMENT_EXT))
            .toList
          val missedLoadMetaDetails: util.List[LoadMetadataDetails] = new util
          .ArrayList[LoadMetadataDetails]()
          segmentFiles.foreach { segmentFileStr =>
            val segmentId = segmentFileStr.substring(0,
              segmentFileStr.indexOf(CarbonCommonConstants.UNDERSCORE, 0))
            val segmentUpdateDetail = updateMetaDetails
              .filter(_.getSegmentName.equalsIgnoreCase(segmentId))
            if (!loadMetaDetails.exists(_.getLoadName.equalsIgnoreCase(segmentId)) &&
                !segmentId.contains(CarbonCommonConstants.POINT)) {
              val segFilePath = CarbonTablePath.getSegmentFilePath(
                carbonTable.getTablePath, segmentFileStr)
              val segmentFile = SegmentFileStore.readSegmentFile(segFilePath)
              val loadMetadataDetail = new LoadMetadataDetails()
              val segmentInfo = segmentFile.getLocationMap.asScala.head._2
              if(!segmentUpdateDetail.isEmpty) {
                loadMetadataDetail.setSegmentStatus(segmentUpdateDetail.head.getSegmentStatus)
                loadMetadataDetail.setModificationOrDeletionTimestamp(segmentUpdateDetail.head
                  .getDeleteDeltaStartTimeAsLong)
              } else {
                loadMetadataDetail.setSegmentStatus(getSegmentStatus(segmentInfo.getStatus))
              }
              loadMetadataDetail.setLoadName(segmentId)
              loadMetadataDetail.setSegmentFile(segmentFileStr)
              val dataIndexSize = CarbonUtil.getDataSizeAndIndexSize(carbonTable
                .getTablePath, new Segment(segmentId, segmentFileStr))
              loadMetadataDetail.setDataSize(dataIndexSize
                .get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toString)
              loadMetadataDetail.setIndexSize(dataIndexSize
                .get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toString)
              loadMetadataDetail.setLoadEndTime(FileFactory
                .getCarbonFile(segFilePath)
                .getLastModifiedTime)
              missedLoadMetaDetails.add(loadMetadataDetail)
            } else if (!segmentUpdateDetail.isEmpty) {
              val loadMetadataDetail = loadMetaDetails
                .filter(_.getLoadName.equalsIgnoreCase(segmentId))
                .head
              loadMetadataDetail.setSegmentStatus(segmentUpdateDetail.head.getSegmentStatus)
              loadMetadataDetail.setModificationOrDeletionTimestamp(segmentUpdateDetail.head
                .getDeleteDeltaStartTimeAsLong)
              loadMetaDetails = loadMetaDetails.filterNot(_.getLoadName.equalsIgnoreCase(segmentId))
              missedLoadMetaDetails.add(loadMetadataDetail)
            }
          }
          missedLoadMetaDetails.addAll(loadMetaDetails.asJava)
          SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
            carbonTable.getTablePath, tableStatusVersion),
            missedLoadMetaDetails.toArray(new Array[LoadMetadataDetails](missedLoadMetaDetails
              .size)))
      }
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
