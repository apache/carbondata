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

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.statusmanager.SegmentStatusManager

/**
 * combine insert, update and delete operations into a single statement,
 * and it will be executed atomically.
 */
case class CarbonDeleteRepeatedCommand(
    database: Option[String],
    tableName: String,
    columnName: String,
    newSegmentIds: SegmentIds,
    oldSegmentIds: Option[SegmentIds]
) extends AtomicRunnableCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Deleted Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    // validate parameter
    Checker.validateTableExists(database, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(database, tableName)(sparkSession)
    val carbonColumn = carbonTable.getColumnByName(carbonTable.getTableName, columnName)
    if (carbonColumn == null) {
      throw new NoSuchFieldException(columnName)
    }
    val segmentRanges =
      prepareSegmentRanges(getValidSegments(carbonTable.getAbsoluteTableIdentifier))
    if (segmentRanges._1.isEmpty || segmentRanges._2.isEmpty) {
      LOGGER.warn("not delete repeated data, because there are no segments to process.")
      Seq.empty[Row]
    } else {
      LOGGER.info(
        s"""delete repeated column ${ carbonColumn.getColName } from table
           | ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName },
           | new segments: ${ segmentRanges._1.map(_.getSegmentNo.mkString(",")) }
           | old segments: ${ segmentRanges._2.map(_.getSegmentNo.mkString(",")) }
           | """.stripMargin)
      deduplicate(
        sparkSession,
        carbonTable,
        carbonColumn,
        segmentRanges._1,
        segmentRanges._2)
    }
  }

  /**
   * get valid segments of the table
   */
  private def getValidSegments(identifier: AbsoluteTableIdentifier): Seq[Segment] = {
    val ssm = new SegmentStatusManager(identifier)
    ssm
      .getValidAndInvalidSegments
      .getValidSegments
      .asScala
  }

  /**
   * prepare newSegments and oldSegments for deduplicate
   */
  private def prepareSegmentRanges(segments: Seq[Segment]): (Seq[Segment], Seq[Segment]) = {
    val newSegments = getSegmentsByIds(segments, newSegmentIds)
    if (newSegments.isEmpty) {
      (Seq.empty[Segment], Seq.empty[Segment])
    } else {
      val oldSegments =
        if (oldSegmentIds.isDefined) {
          getSegmentsByIds(segments, oldSegmentIds.get)
        } else {
          val segmentMap = newSegments.map { segment =>
            (segment.getSegmentNo, segment)
          }.toMap
          segments.filter { segment =>
            !segmentMap.contains(segment.getSegmentNo)
          }
        }
      (newSegments, oldSegments)
    }
  }

  /**
   * get Segment Seq from segment id Seq
   */
  private def getSegmentsByIds(
      segments: Seq[Segment],
      segmentIds: SegmentIds): Seq[Segment] = {
    if (segmentIds.isRange) {
      val newStart = new java.math.BigDecimal(segmentIds.segmentRange._1)
      val newEnd = new java.math.BigDecimal(segmentIds.segmentRange._2)
      val updatedSegmentRange = if (newStart.compareTo(newEnd) <= 0) {
        (newStart, newEnd)
      } else {
        (newEnd, newStart)
      }
      segments
        .map { segment =>
          (segment, new java.math.BigDecimal(segment.getSegmentNo))
        }
        .filter { segment =>
          segment._2.compareTo(updatedSegmentRange._1) >= 0 &&
          segment._2.compareTo(updatedSegmentRange._2) <= 0
        }
        .map(_._1)
    } else {
      val segmentIdsMap = segmentIds.segments.map((_, true)).toMap
      segments.filter { segment =>
        segmentIdsMap.contains(segment.getSegmentNo)
      }
    }
  }

  /**
   * delete repeated data from newSegments if the data also exist in oldSegments
   */
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
    deleteCommand.deleteSegments = newSegments.map(_.getSegmentNo).mkString(",")
    deleteCommand.repeatedSegments = oldSegments.map(_.getSegmentNo).mkString(",")
    deleteCommand.run(sparkSession)
  }

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }

  override protected def opName: String = "ALTER TABLE DISTINCT"
}

/**
 * wrap segment id range and set
 */
case class SegmentIds(isRange: Boolean, segmentRange: (String, String), segments: Seq[String])
