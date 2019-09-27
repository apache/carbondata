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
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.statusmanager.SegmentStatusManager

/**
 * combine insert, update and delete operations into a single statement,
 * and it will be executed atomically.
 */
case class CarbonDeleteRepeatedCommand(
    database: Option[String],
    tableName: String,
    columnName: String,
    newSegmentRange: (String, String),
    oldSegmentRange: Option[(String, String)]
) extends AtomicRunnableCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Deleted Row Count", LongType, nullable = false)())
  }

  private def getSegmentRange(segmentRange: (String, String)): (Float, Float) = {
    val newStart =  java.lang.Float.valueOf(segmentRange._1)
    val newEnd =  java.lang.Float.valueOf(segmentRange._2)
    (Math.min(newStart, newEnd), Math.max(newStart, newEnd))
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
      .map{ segment =>
        (segment, java.lang.Float.valueOf(segment.getSegmentNo))
      }
    if (segments.isEmpty) {
      throw new MalformedCarbonCommandException(s"table has no segments")
    }
    val updatedSegmentRange = getSegmentRange(newSegmentRange)
    val newSegments =
      segments.filter { segment =>
        segment._2 >= updatedSegmentRange._1 && segment._2 <= updatedSegmentRange._2
      }
        .map(_._1)
    if (newSegments.isEmpty) {
      Seq.empty[Row]
    } else {
      val oldSegments =
        if (oldSegmentRange.isDefined) {
          val updatedOldSegmentRange = getSegmentRange(oldSegmentRange.get)
          segments.filter { segment =>
            segment._2 >= updatedOldSegmentRange._1 && segment._2 <= updatedOldSegmentRange._2
          }.map(_._1)
        } else {
          segments.filter { segment =>
            segment._2 > updatedSegmentRange._2 || segment._2 < updatedSegmentRange._1
          }.map(_._1)
        }
      if (oldSegments.isEmpty) {
        Seq.empty[Row]
      } else {
        deduplicate(sparkSession, carbonTable, carbonColumn, newSegments, oldSegments)
      }
    }
  }

  private def deduplicate(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      carbonColumn: CarbonColumn,
      newSegments: Seq[Segment],
      oldSegments: Seq[Segment]): Seq[Row] = {
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
    deleteCommand.deleteSegments = newSegments.mkString(",")
    deleteCommand.repeatedSegments = oldSegments.mkString(",")
    deleteCommand.run(sparkSession)
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }

  override protected def opName: String = "ALTER TABLE DISTINCT"
}
