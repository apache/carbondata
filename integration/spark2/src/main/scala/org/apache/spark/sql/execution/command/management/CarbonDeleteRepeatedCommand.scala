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
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, Checker}
import org.apache.spark.sql.execution.command.mutation.CarbonProjectForDeleteCommand
import org.apache.spark.sql.hive.CarbonIUDAnalysisRule
import org.apache.spark.sql.types.LongType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties

/**
 * combine insert, update and delete operations into a single statement,
 * and it will be executed atomically.
 */
case class CarbonDeleteRepeatedCommand(
    database: Option[String],
    tableName: String,
    columnName: String,
    segmentId: String
) extends AtomicRunnableCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Deleted Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // validate parameter
    Checker.validateTableExists(database, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(database, tableName)(sparkSession)
    val carbonColumn = carbonTable.getColumnByName(carbonTable.getTableName, columnName)
    if (carbonColumn == null) {
      throw new NoSuchFieldException(columnName)
    }
    val ssm = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
    val segments = ssm
      .getValidAndInvalidSegments
      .getValidSegments
      .asScala
    if (segments.isEmpty) {
      throw new MalformedCarbonCommandException(s"table has no data")
    }
    val segment = segments.find(_.getSegmentNo.equals(segmentId))
    if (segment.isEmpty) {
      throw new MalformedCarbonCommandException(s"Invalid segment id: $segmentId")
    }
    // get the segment with repeated data
    val loadEndTime = segment.get.getLoadMetadataDetails.getLoadEndTime
    val oldSegments = segments
      .filter(_.getLoadMetadataDetails.getLoadStartTime <= loadEndTime)
      .filter(!_.getSegmentNo.equals(segment.get.getSegmentNo))
    if (oldSegments.isEmpty) {
      throw new MalformedCarbonCommandException(
        s"table has no other segments before the specify segment")
    }
    val segmentNumber = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.DELETE_REPEATED_SEGMENT_NUMBER,
      CarbonCommonConstants.DELETE_REPEATED_SEGMENT_NUMBER_DEFAULT).toInt

    val repeatedSegment =
      oldSegments
        .sortBy(-_.getLoadMetadataDetails.getLoadStartTime)
        .take(segmentNumber)
        .map(_.getSegmentNo)
        .mkString(",")

    val fullName = carbonTable.getDatabaseName + "." + carbonTable.getTableName
    val colName = carbonColumn.getColName
    val selectSQL =
      s"""
         | select tupleId
         |  from $fullName t
         |  where exists(select 1 from $fullName s where t.$colName = s.$colName)
       """.stripMargin
    val relation = UnresolvedRelation(TableIdentifier(
      carbonTable.getTableName, Option(carbonTable.getDatabaseName)))
    val deleteCommand =
    CarbonIUDAnalysisRule(sparkSession)
      .processDeleteRecordsQuery(selectSQL, Some("t"), relation)
      .asInstanceOf[CarbonProjectForDeleteCommand]
    deleteCommand.isDeleteRepeat = true
    deleteCommand.deleteSegment = segment.get.getSegmentNo
    deleteCommand.repeatedSegments = repeatedSegment
    deleteCommand.run(sparkSession)
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }

  override protected def opName: String = "ALTER TABLE DISTINCT"
}
