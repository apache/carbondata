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
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.events.{DeleteSegmentByIdPostEvent, DeleteSegmentByIdPreEvent, withEvents}

/**
 * A command for delete by remaining number.
 * In general, keep the latest segment.
 *
 * @param remaining expected remaining quantity after deletion
 */
case class CarbonDeleteLoadByRemainNumberCommand(
    remaining: Int,
    databaseNameOp: Option[String],
    tableName: String)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("remaining number" -> remaining.toString))
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // if insert overwrite in progress, do not allow delete segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }

    val segments = CarbonStore.readSegments(carbonTable.getTablePath, showHistory = false, None)

    var deleteSegmentIds = List[String]()
    if (carbonTable.isHivePartitionTable) {
      segments.map(segment =>
        (CarbonStore.getPartitions(carbonTable.getTablePath, segment), segment))
        .groupBy(m => m._1)
        .foreach(elem => {
          val ids = elem._2.map(p => p._2).filter(segment =>
            segment.getSegmentStatus == SegmentStatus.SUCCESS ||
              segment.getSegmentStatus == SegmentStatus.COMPACTED)
            .sortBy(_.getLoadStartTime)
            .map(_.getLoadName)
            .reverse
            .drop(remaining).toList
          deleteSegmentIds = List.concat(deleteSegmentIds, ids)
      })
    } else {
      deleteSegmentIds = segments.filter(segment =>
        segment.getSegmentStatus == SegmentStatus.SUCCESS ||
          segment.getSegmentStatus == SegmentStatus.COMPACTED)
        .sortBy(_.getLoadStartTime)
        .map(_.getLoadName)
        .reverse
        .drop(remaining).toList
    }

    if (deleteSegmentIds.isEmpty) {
      return Seq.empty
    }

    withEvents(DeleteSegmentByIdPreEvent(carbonTable, deleteSegmentIds, sparkSession),
      DeleteSegmentByIdPostEvent(carbonTable, deleteSegmentIds, sparkSession)) {
      CarbonStore.deleteLoadById(
        deleteSegmentIds,
        CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession),
        tableName,
        carbonTable
      )
    }
    Seq.empty
  }

  override protected def opName: String = "DELETE SEGMENT BY REMAIN_NUMBER"
}
