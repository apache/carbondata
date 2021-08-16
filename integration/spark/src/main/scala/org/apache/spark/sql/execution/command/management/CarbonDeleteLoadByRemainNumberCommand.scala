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
    remaining: String,
    databaseNameOp: Option[String],
    tableName: String)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("remaining number" -> remaining))
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // if insert overwrite in progress, do not allow delete segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }

    val segments = CarbonStore.readSegments(carbonTable.getTablePath, showHistory = false, None)
    if (segments.length == remaining.toInt) {
      return Seq.empty
    }

    // Through the remaining number, get the delete id
    val deleteSegmentIds = segments.filter(segment => segment.getSegmentStatus != SegmentStatus.MARKED_FOR_DELETE)
      .sortBy(_.getLoadStartTime)
      .map(_.getLoadName)
      .reverse
      .drop(remaining.toInt)

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