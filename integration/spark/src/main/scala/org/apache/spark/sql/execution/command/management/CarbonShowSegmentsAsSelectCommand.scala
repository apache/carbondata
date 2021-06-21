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

import org.apache.spark.sql.{AnalysisException, CarbonEnv, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, LongType, StringType}

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.api.CarbonStore.readSegments
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails

case class SegmentRow(
    id: String, status: String, loadStartTime: String, timeTakenMs: String, partitions: Seq[String],
    dataSize: String, indexSize: String, mergedToId: String, format: String, path: String,
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

  val tempViewName: String = makeTempViewName(carbonTable)

  val df: DataFrame = createDataFrame

  var attrList: Seq[Attribute] = Seq()

  override def output: Seq[Attribute] = {
    sparkSession.sessionState.sqlParser.parsePlan(query).find(_.isInstanceOf[Project]) match {
      case Some(project: Project) =>
        val projectList = project.projectList
        if (projectList.exists(_.isInstanceOf[UnresolvedStar])) {
          attrList = df.queryExecution.analyzed.output
        } else {
          attrList = projectList.map(x => AttributeReference(x.name, DataTypes.StringType,
              nullable = false)())
        }
      case _ =>
    }
    attrList
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    try {
      setAuditTable(carbonTable)
      if (!carbonTable.getTableInfo.isTransactionalTable) {
        throw new MalformedCarbonCommandException(
          "Unsupported operation on non transactional table")
      }
      checkIfTableExist(sparkSession, tempViewName)
      df.createTempView(tempViewName)
      sparkSession.sql(query).collect()
    } catch {
      case an: AnalysisException => throw an
      case ex: Throwable =>
        throw new MalformedCarbonCommandException("failed to run query: " + ex.getMessage)
    } finally {
      sparkSession.catalog.dropTempView(tempViewName)
    }
  }

  override protected def opName: String = "SHOW SEGMENTS"

  private def createDataFrame: DataFrame = {
    val tableStagePath = carbonTable.getStagePath
    val tablePath = carbonTable.getTablePath
    var rows: Seq[SegmentRow] = Seq()
    if (withStage) {
      val stageRows = CarbonShowSegmentsCommand.showStages(tableStagePath)
      if (stageRows.nonEmpty) {
        rows = stageRows.map(
          stageRow =>
            SegmentRow (
              stageRow.getString(0),
              stageRow.getString(1),
              stageRow.getString(2),
              "-1",
              Seq(stageRow.getString(4)),
              stageRow.getString(5),
              stageRow.getString(6),
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
    makeDfFromRows(sparkSession, tempViewName,
      carbonTable, segments, rows)
  }

  /**
   * Generate temp view name for the query to execute
   */
  private def makeTempViewName(carbonTable: CarbonTable): String = {
    s"${carbonTable.getTableName}_segments"
  }

  private def makeDfFromRows(
      sparkSession: SparkSession,
      tempViewName: String,
      carbonTable: CarbonTable,
      segments: Array[LoadMetadataDetails],
      rows: Seq[SegmentRow]): DataFrame = {

    // populate a DataFrame containing all segment information
    val tablePath = carbonTable.getTablePath
    val segmentRowView = rows ++ segments.toSeq.map { segment =>
      val mergedToId = CarbonStore.getMergeTo(segment)
      val path = CarbonStore.getExternalSegmentPath(segment)
      val startTime = CarbonStore.getLoadStartTime(segment)
      val endTime = CarbonStore.getLoadEndTime(segment)
      val timeTaken = CarbonStore.getLoadTimeTakenAsMillis(segment)
      val (dataSize, indexSize) = CarbonStore.getDataAndIndexSize(tablePath, segment)
      val partitions = if (carbonTable.isHivePartitionTable) {
        CarbonStore.getPartitions(tablePath, segment)
      } else {
        Seq.empty
      }
      SegmentRow(
        segment.getLoadName,
        segment.getSegmentStatus.toString,
        startTime,
        timeTaken.toString,
        partitions,
        dataSize.toString,
        indexSize.toString,
        mergedToId,
        segment.getFileFormat.toString,
        path,
        endTime,
        if (segment.getSegmentFile == null) "NA" else segment.getSegmentFile)
    }

    // create a temp view using the populated DataFrame and execute the query on it
    val df = sparkSession.createDataFrame(segmentRowView)
    df
  }

  private def checkIfTableExist(sparkSession: SparkSession, tempViewName: String): Unit = {
    if (sparkSession.catalog.tableExists(tempViewName)) {
      throw new MalformedCarbonCommandException(s"$tempViewName already exists, " +
                                                s"can not show segment by query")
    }
  }

}
