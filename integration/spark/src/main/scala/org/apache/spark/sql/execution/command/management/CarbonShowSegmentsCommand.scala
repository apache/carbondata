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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.api.CarbonStore.{getDataAndIndexSize, getLoadStartTime, getLoadTimeTaken, getPartitions, readSegments}
import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails

case class CarbonShowSegmentsCommand(
    databaseNameOp: Option[String],
    tableName: String,
    showHistory: Boolean = false)
  extends DataCommand {

  // add new columns of show segments at last
  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("ID", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Load Start Time", StringType, nullable = false)(),
      AttributeReference("Load Time Taken", StringType, nullable = true)(),
      AttributeReference("Partition", StringType, nullable = true)(),
      AttributeReference("Data Size", StringType, nullable = false)(),
      AttributeReference("Index Size", StringType, nullable = false)(),
      AttributeReference("File Format", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    val tablePath = carbonTable.getTablePath
    val segments = readSegments(tablePath, showHistory)
    if (segments.nonEmpty) {
      showBasic(segments, tablePath)
    } else {
      Seq.empty
    }
  }

  override protected def opName: String = "SHOW SEGMENTS"

  private def showBasic(
      allSegments: Array[LoadMetadataDetails],
      tablePath: String): Seq[Row] = {
    val segments = allSegments.sortWith { (l1, l2) =>
      java.lang.Double.parseDouble(l1.getLoadName) >
      java.lang.Double.parseDouble(l2.getLoadName)
    }

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
