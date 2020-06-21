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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.api.CarbonStore.{getDataAndIndexSize, getLoadStartTime, getLoadTimeTaken, getPartitions, readSegments, readStages}
import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, StageInput}
import org.apache.carbondata.core.util.path.CarbonTablePath

import org.apache.hadoop.conf.Configuration

case class CarbonShowSegmentsCommand(
    databaseNameOp: Option[String],
    tableName: String,
    limit: Option[Int],
    includeStage: Boolean = false,
    showHistory: Boolean = false)
  extends DataCommand {

  // add new columns of show segments at last
  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("ID", StringType, nullable = true)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Load Start Time", StringType, nullable = false)(),
      AttributeReference("Load Time Taken", StringType, nullable = true)(),
      AttributeReference("Partition", StringType, nullable = true)(),
      AttributeReference("Data Size", StringType, nullable = false)(),
      AttributeReference("Index Size", StringType, nullable = false)(),
      AttributeReference("File Format", StringType, nullable = true)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    val tablePath = carbonTable.getTablePath
    val segments = readSegments(tablePath, showHistory, limit)
    var rows = if (segments.nonEmpty) {
      showBasic(segments, tablePath)
    } else {
      Seq[Row]()
    }
    if (includeStage) {
      rows = rows ++ CarbonShowSegmentsCommand.showStages(rows.length, tablePath,
        sparkSession.sessionState.newHadoopConf(), limit)
    }
    rows
  }

  override protected def opName: String = "SHOW SEGMENTS"

  private def showBasic(
      segments: Array[LoadMetadataDetails],
      tablePath: String): Seq[Row] = {
    segments
      .map { segment =>
        val startTime = getLoadStartTime(segment)
        val timeTaken = getLoadTimeTaken(segment)
        val (dataSize, indexSize) = getDataAndIndexSize(tablePath, segment)
        val partitions = getPartitions(tablePath, segment)
        val partitionString = if (partitions.size == 1) {
          partitions.head
        } else if (partitions.size > 1) {
          partitions.head + ", ..."
        } else {
          "NA"
        }
        Row(
          segment.getLoadName,
          segment.getSegmentStatus.getMessage,
          startTime,
          timeTaken,
          partitionString,
          Strings.formatSize(dataSize.toFloat),
          Strings.formatSize(indexSize.toFloat),
          segment.getFileFormat.toString)
      }.toSeq
  }
}

object CarbonShowSegmentsCommand {

  def showStages(
      segmentCount: Int,
      tablePath: String,
      configuration: Configuration,
      limit: Option[Int]): Seq[Row] = {
    if (limit.isEmpty) {
      toRows(readStages(tablePath, configuration, None))
    } else if (segmentCount < limit.get) {
      toRows(readStages(tablePath, configuration, Some(limit.get - segmentCount)))
    } else {
      Seq()
    }
  }

  private def toRows(stages: Seq[StageInput]): Seq[Row] = {
    var rows = Seq[Row]()
    stages.foreach(
      stage =>
        rows = rows ++ toRows(stage)
    )
    rows
  }

  private def toRows(stage: StageInput): Seq[Row] = {
    if (stage.getFiles != null) {
      // Non-partition stage
      Seq(
        Row(
          null,
          "Unload",
          new java.sql.Timestamp(stage.getCreateTime).toString,
          null,
          "NA",
          Strings.formatSize(countDataFileSize(stage.getFiles)),
          Strings.formatSize(countIndexFileSize(stage.getFiles)),
          null)
      )
    } else {
      // Partition stage
      stage.getLocations.asScala.map{
        location =>
          val partitions = location.getPartitions.asScala
          val partitionString = if (partitions.size == 1) {
            partitions.head._1 + "=" + partitions.head._2
          } else if (partitions.size > 1) {
            partitions.head._1 + "=" + partitions.head._2 + ", ..."
          } else {
            "NA"
          }
          Row(
            null,
            "Unload",
            new java.sql.Timestamp(stage.getCreateTime).toString,
            null,
            partitionString,
            Strings.formatSize(countDataFileSize(location.getFiles)),
            Strings.formatSize(countDataFileSize(location.getFiles)),
            null)
      }
    }
  }

  private def countDataFileSize(files: java.util.Map[java.lang.String, java.lang.Long]): Long = {
    var fileSize: Long = 0
    import scala.collection.JavaConverters._
    files.asScala.foreach(
      file =>
        if (file._1.endsWith(CarbonTablePath.CARBON_DATA_EXT))
          fileSize += file._2
    )
    fileSize
  }

  private def countIndexFileSize(files: java.util.Map[java.lang.String, java.lang.Long]): Long = {
    var fileSize: Long = 0
    files.asScala.foreach(
      file =>
        if (file._1.endsWith(CarbonTablePath.INDEX_FILE_EXT) ||
          file._1.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT))
          fileSize += file._2
    )
    fileSize
  }
}