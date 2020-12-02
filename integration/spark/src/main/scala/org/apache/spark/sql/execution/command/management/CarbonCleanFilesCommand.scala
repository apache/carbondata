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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events._
import org.apache.carbondata.trash.DataTrashManager

/**
 * Clean data in table
 * If table name is specified and forceTableClean is false, it will clean garbage
 * segment (MARKED_FOR_DELETE state).
 * If table name is specified and forceTableClean is true, it will delete all data
 * in the table.
 * If table name is not provided, it will clean garbage segment in all tables.
 */
case class CarbonCleanFilesCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String] = Map.empty,
    isInternalCleanCall: Boolean = false)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    val force = options.getOrElse("force", "false")
    setAuditInfo(Map(
      "force" -> force,
      "internal" -> isInternalCleanCall.toString))
    // if insert overwrite in progress, do not allow delete segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "clean file")
    }
    // only support transactional table
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    val operationContext = new OperationContext
    val cleanFilesPreEvent: CleanFilesPreEvent = CleanFilesPreEvent(carbonTable, sparkSession)
    OperationListenerBus.getInstance.fireEvent(cleanFilesPreEvent, operationContext)

    val partitions = CarbonFilters.getPartitions( Seq.empty[Expression], sparkSession, carbonTable)
    DataTrashManager.cleanGarbageData(
      carbonTable = carbonTable,
      force = force.toBoolean,
      partitionSpecs = partitions)

    val cleanFilesPostEvent: CleanFilesPostEvent =
      CleanFilesPostEvent(carbonTable, sparkSession, options)
    OperationListenerBus.getInstance.fireEvent(cleanFilesPostEvent, operationContext)
    Seq.empty
  }

  override protected def opName: String = "CLEAN FILES"
}
