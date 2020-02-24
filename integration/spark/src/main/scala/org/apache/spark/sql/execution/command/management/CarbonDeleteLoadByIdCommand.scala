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
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events.{DeleteSegmentByIdPostEvent, DeleteSegmentByIdPreEvent, OperationContext, OperationListenerBus}

case class CarbonDeleteLoadByIdCommand(
    loadIds: Seq[String],
    databaseNameOp: Option[String],
    tableName: String)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("segmentIds" -> loadIds.mkString(", ")))
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // if insert overwrite in progress, do not allow delete segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "delete segment")
    }

    val operationContext = new OperationContext
    val deleteSegmentByIdPreEvent: DeleteSegmentByIdPreEvent =
      DeleteSegmentByIdPreEvent(carbonTable,
        loadIds,
        sparkSession)
    OperationListenerBus.getInstance.fireEvent(deleteSegmentByIdPreEvent, operationContext)

    CarbonStore.deleteLoadById(
      loadIds,
      CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession),
      tableName,
      carbonTable
    )

    val deleteSegmentPostEvent: DeleteSegmentByIdPostEvent =
      DeleteSegmentByIdPostEvent(carbonTable,
        loadIds,
        sparkSession)
    OperationListenerBus.getInstance.fireEvent(deleteSegmentPostEvent, operationContext)
    Seq.empty
  }

  override protected def opName: String = "DELETE SEGMENT BY ID"
}
