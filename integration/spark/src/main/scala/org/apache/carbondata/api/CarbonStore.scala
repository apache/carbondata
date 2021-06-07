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

import java.io.{DataInputStream, FileNotFoundException, InputStreamReader}
import java.time.{Duration, Instant}
import java.util
import java.util.{Collections, Comparator}

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.CarbonToSparkAdapter

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatusManager, StageInput}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.streaming.segment.StreamSegment

object CarbonStore {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val READ_FILE_RETRY_TIMES = 3

  val READ_FILE_RETRY_INTERVAL = 1000

  def readSegments(
      tablePath: String,
      showHistory: Boolean,
      limit: Option[Int]): Array[LoadMetadataDetails] = {
    val metaFolder = CarbonTablePath.getMetadataPath(tablePath)
    var segmentsMetadataDetails = if (showHistory) {
      SegmentStatusManager.readLoadMetadata(metaFolder) ++
      SegmentStatusManager.readLoadHistoryMetadata(metaFolder)
    } else {
      SegmentStatusManager.readLoadMetadata(metaFolder)
    }
    if (!showHistory) {
      segmentsMetadataDetails = segmentsMetadataDetails
        .filter(_.getVisibility.equalsIgnoreCase("true"))
      segmentsMetadataDetails = segmentsMetadataDetails.sortWith { (l1, l2) =>
        java.lang.Double.parseDouble(l1.getLoadName) >
        java.lang.Double.parseDouble(l2.getLoadName)
      }
    }

    if (limit.isDefined) {
      segmentsMetadataDetails.slice(0, limit.get)
    } else {
      segmentsMetadataDetails
    }
  }

  /**
   * Read stage files and return input files
   */
  def readStages(tableStagePath: String): Seq[StageInput] = {
    val stageFiles = listStageFiles(tableStagePath)
    var output = Collections.synchronizedList(new util.ArrayList[StageInput]())
    output.addAll(readStageInput(tableStagePath, stageFiles._1,
      StageInput.StageStatus.Unload).asJavaCollection)
    output.addAll(readStageInput(tableStagePath, stageFiles._2,
      StageInput.StageStatus.Loading).asJavaCollection)
    Collections.sort(output, new Comparator[StageInput]() {
      def compare(stageInput1: StageInput, stageInput2: StageInput): Int = {
        (stageInput2.getCreateTime - stageInput1.getCreateTime).intValue()
      }
    })
    output.asScala
  }

  /**
   * Read stage files and return input files
   */
  def readStageInput(
      tableStagePath: String,
      stageFiles: Seq[CarbonFile],
      status: StageInput.StageStatus): Seq[StageInput] = {
    val gson = new Gson()
    val output = Collections.synchronizedList(new util.ArrayList[StageInput]())
    stageFiles.foreach { stage =>
      val filePath = tableStagePath + CarbonCommonConstants.FILE_SEPARATOR + stage.getName
      var stream: DataInputStream = null
      try {
        stream = FileFactory.getDataInputStream(filePath)
        var retry = READ_FILE_RETRY_TIMES
        breakable {
          while (retry > 0) {
            try {
              val stageInput = gson.fromJson(new InputStreamReader(stream), classOf[StageInput])
              stageInput.setCreateTime(stage.getLastModifiedTime)
              stageInput.setStatus(status)
              output.add(stageInput)
              break()
            } catch {
              case _ : FileNotFoundException =>
                LOGGER.warn(s"The stage file $filePath does not exist")
                break()
              case ex: Exception => retry -= 1
                if (retry > 0) {
                  LOGGER.warn(s"The stage file $filePath can't be read, retry " +
                    s"$retry times: ${ex.getMessage}")
                  Thread.sleep(READ_FILE_RETRY_INTERVAL)
                } else {
                  LOGGER.error(s"The stage file $filePath can't be" +
                    s" read: ${ex.getMessage}")
                  throw ex
                }
            }
          }
        }
      } finally {
        if (stream != null) {
          stream.close()
        }
      }
    }
    output.asScala
  }

  /*
   * Collect all stage files and matched success files and loading files.
   * return unloaded stage files and loading stage files in the end.
   */
  def listStageFiles(
        loadDetailsDir: String): (Array[CarbonFile], Array[CarbonFile]) = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir)
    if (dir.exists()) {
      // 1. List all files in the stage dictionary.
      val allFiles = dir.listFiles()
      val allFileNames = allFiles.map(file => file.getName)

      // 2. Get StageFile list.
      // Firstly, get the stage files in the stage dictionary.
      //        which exclude the success files and loading files
      // Second,  only collect the stage files having success tag.
      val stageFiles = allFiles.filterNot { file =>
        file.getName.endsWith(CarbonTablePath.SUCCESS_FILE_SUFFIX)
      }.filterNot { file =>
        file.getName.endsWith(CarbonTablePath.LOADING_FILE_SUFFIX)
      }.filter { file =>
        allFileNames.contains(file.getName + CarbonTablePath.SUCCESS_FILE_SUFFIX)
      }.sortWith {
        (file1, file2) => file1.getLastModifiedTime > file2.getLastModifiedTime
      }
      // 3. Get the unloaded stage files, which haven't loading tag.
      val unloadedFiles = stageFiles.filterNot { file =>
        allFileNames.contains(file.getName + CarbonTablePath.LOADING_FILE_SUFFIX)
      }
      // 4. Get the loading stage files, which have loading tag.
      val loadingFiles = stageFiles.filter { file =>
        allFileNames.contains(file.getName + CarbonTablePath.LOADING_FILE_SUFFIX)
      }
      (unloadedFiles, loadingFiles)
    } else {
      (Array.empty, Array.empty)
    }
  }

  def getPartitions(tablePath: String, load: LoadMetadataDetails): Seq[String] = {
    val segmentFile = SegmentFileStore.readSegmentFile(
      CarbonTablePath.getSegmentFilePath(tablePath, load.getSegmentFile))
    if (segmentFile == null) {
      return Seq.empty
    }
    val locationMap = segmentFile.getLocationMap
    if (locationMap != null) {
      locationMap.asScala.map {
        case (_, detail) =>
          s"{${ detail.getPartitions.asScala.mkString(",") }}"
      }.toSeq
    } else {
      Seq.empty
    }
  }

  def getMergeTo(load: LoadMetadataDetails): String = {
    if (load.getMergedLoadName != null) {
      load.getMergedLoadName
    } else {
      "NA"
    }
  }

  def getExternalSegmentPath(load: LoadMetadataDetails): String = {
    if (StringUtils.isNotEmpty(load.getPath)) {
      load.getPath
    } else {
      "NA"
    }
  }

  def getLoadStartTime(load: LoadMetadataDetails): String = {
    val startTime =
      if (load.getLoadStartTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
        "NA"
      } else {
        new java.sql.Timestamp(load.getLoadStartTime).toString
      }
    startTime
  }

  def getLoadEndTime(load: LoadMetadataDetails): String = {
    val endTime =
      if (load.getLoadStartTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
        "NA"
      } else {
        new java.sql.Timestamp(load.getLoadEndTime).toString
      }
    endTime
  }

  def getLoadTimeTaken(load: LoadMetadataDetails): String = {
    if (load.getLoadEndTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
      "NA"
    } else {
      Duration.between(
        Instant.ofEpochMilli(load.getLoadStartTime),
        Instant.ofEpochMilli(load.getLoadEndTime)
      ).toString.replace("PT", "")
    }
  }

  def getLoadTimeTakenAsMillis(load: LoadMetadataDetails): Long = {
    if (load.getLoadEndTime == CarbonCommonConstants.SEGMENT_LOAD_TIME_DEFAULT) {
      // loading in progress
      -1L
    } else {
      load.getLoadEndTime - load.getLoadStartTime
    }
  }

  def getDataAndIndexSize(
      tablePath: String,
      load: LoadMetadataDetails): (Long, Long) = {
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
        (if (load.getDataSize == null) -1L else load.getDataSize.toLong, -1L)
      } else {
        (if (load.getDataSize == null) -1L else load.getDataSize.toLong,
          if (load.getIndexSize == null) -1L else load.getIndexSize.toLong)
      }
    }
    (dataSize, indexSize)
  }

  // validates load ids
  private def validateLoadIds(loadIds: Seq[String]): Unit = {
    if (loadIds.isEmpty) {
      val errorMessage = "Error: Segment id(s) should not be empty."
      throw new MalformedCarbonCommandException(errorMessage)
    }
  }

  // TODO: move dbName and tableName to caller, caller should handle the log and error
  def deleteLoadById(
      loadIds: Seq[String],
      dbName: String,
      tableName: String,
      carbonTable: CarbonTable): Unit = {

    validateLoadIds(loadIds)

    val path = carbonTable.getMetadataPath

    try {
      val invalidLoadIds = SegmentStatusManager.updateDeletionStatus(
        carbonTable.getAbsoluteTableIdentifier, loadIds.asJava, path).asScala
      if (invalidLoadIds.isEmpty) {
        LOGGER.info(s"Delete segment by Id is successful for $dbName.$tableName.")
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
      CarbonToSparkAdapter.stringToTimestamp(timestamp) match {
        case Some(value) => value
        case _ =>
          throw new RuntimeException
      }
    } catch {
      case _: Exception =>
        val errorMessage = "Error: Invalid load start time format: " + timestamp
        throw new MalformedCarbonCommandException(errorMessage)
    }
  }

}
