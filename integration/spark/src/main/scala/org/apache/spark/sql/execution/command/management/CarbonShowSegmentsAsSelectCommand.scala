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

import org.apache.spark.sql.{CarbonEnv, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.api.CarbonStore.readSegments
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails

case class SegmentRow(
    id: String, status: String, loadStartTime: String, timeTakenMs: Long, partitions: Seq[String],
    dataSize: Long, indexSize: Long, mergedToId: String, format: String, path: String,
    loadEndTime: String, segmentFileName: String)

case class CarbonShowSegmentsAsSelectCommand(
    databaseNameOp: Option[String],
    tableName: String,
    query: String,
    limit: Option[Int],
    showHistory: Boolean = false,
    withStage: Boolean = false)
  extends DataCommand {

  private lazy val sparkSession = SparkSession.getActiveSession.get
  private lazy val carbonTable = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
  }
  private lazy val df = createDataFrame

  override def output: Seq[Attribute] = {
    df.queryExecution.analyzed.output.map { attr =>
      AttributeReference(attr.name, attr.dataType, nullable = true)()
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    try {
      setAuditTable(carbonTable)
      if (!carbonTable.getTableInfo.isTransactionalTable) {
        throw new MalformedCarbonCommandException(
          "Unsupported operation on non transactional table")
      }
      df.collect()
    } catch {
      case ex: Throwable =>
        throw new MalformedCarbonCommandException("failed to run query: " + ex.getMessage)
    } finally {
      sparkSession.catalog.dropTempView(makeTempViewName(carbonTable))
    }
  }

  override protected def opName: String = "SHOW SEGMENTS"

  private def createDataFrame: DataFrame = {
    val tablePath = carbonTable.getTablePath
    var rows: Seq[SegmentRow] = Seq()
    if (withStage) {
      val stageRows = CarbonShowSegmentsCommand.showStages(tablePath)
      if (stageRows.nonEmpty) {
        rows = stageRows.map(
          stageRow =>
            SegmentRow (
              stageRow.getString(0),
              stageRow.getString(1),
              stageRow.getString(2),
              -1,
              Seq(stageRow.getString(4)),
              stageRow.getString(5).toLong,
              stageRow.getString(6).toLong,
              null,
              stageRow.getString(7),
              null,
              null,
              null
            )
        )
      }
    }

    val segments = readSegments(tablePath, showHistory, limit)
    val tempViewName = makeTempViewName(carbonTable)
    registerSegmentRowView(sparkSession, tempViewName,
      carbonTable, segments, rows)
    try {
      sparkSession.sql(query)
    } catch {
      case t: Throwable =>
        sparkSession.catalog.dropTempView(tempViewName)
        throw t
    }
  }

  /**
   * Generate temp view name for the query to execute
   */
  private def makeTempViewName(carbonTable: CarbonTable): String = {
    s"${carbonTable.getTableName}_segments"
  }

  private def registerSegmentRowView(
      sparkSession: SparkSession,
      tempViewName: String,
      carbonTable: CarbonTable,
      segments: Array[LoadMetadataDetails],
      rows: Seq[SegmentRow]): Unit = {

    // populate a DataFrame containing all segment information
    val tablePath = carbonTable.getTablePath
    val segmentRowView = rows ++ segments.toSeq.map { segment =>
      val mergedToId = CarbonStore.getMergeTo(segment)
      val path = CarbonStore.getExternalSegmentPath(segment)
      val startTime = CarbonStore.getLoadStartTime(segment)
      val endTime = CarbonStore.getLoadEndTime(segment)
      val timeTaken = CarbonStore.getLoadTimeTakenAsMillis(segment)
      val (dataSize, indexSize) = CarbonStore.getDataAndIndexSize(tablePath, segment)
      val partitions = CarbonStore.getPartitions(tablePath, segment)
      SegmentRow(
        segment.getLoadName,
        segment.getSegmentStatus.toString,
        startTime,
        timeTaken,
        partitions,
        dataSize,
        indexSize,
        mergedToId,
        segment.getFileFormat.toString,
        path,
        endTime,
        if (segment.getSegmentFile == null) "NA" else segment.getSegmentFile)
    }

    // create a temp view using the populated DataFrame and execute the query on it
    val df = sparkSession.createDataFrame(segmentRowView)
    checkIfTableExist(sparkSession, tempViewName)
    df.createOrReplaceTempView(tempViewName)
  }

  private def checkIfTableExist(sparkSession: SparkSession, tempViewName: String): Unit = {
    if (sparkSession.catalog.tableExists(tempViewName)) {
      throw new MalformedCarbonCommandException(s"$tempViewName already exists, " +
                                                s"can not show segment by query")
    }
  }

}
