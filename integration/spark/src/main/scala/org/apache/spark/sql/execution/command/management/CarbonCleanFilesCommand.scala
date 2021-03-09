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

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.execution.command.{Checker, DataCommand}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.ByteUtil
import org.apache.carbondata.events._
import org.apache.carbondata.trash.DataTrashManager

/**
 * Clean garbage data in table, it invokes TrashDataManager.cleanGarbageData to implement it.
 */
case class CarbonCleanFilesCommand(
    databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String] = Map.empty,
    isInternalCleanCall: Boolean = false)
  extends DataCommand {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val isDryRun: Boolean = options.getOrElse("dryrun", "false").toBoolean
  val showStats: Boolean = if (isInternalCleanCall) {
    false
  } else {
    options.getOrElse("statistics", "true").toBoolean
  }

  override def output: Seq[AttributeReference] = {
    if (isDryRun) {
      // dry run operation
      Seq(
        AttributeReference("Size To Be Freed", StringType, nullable = false)(),
        AttributeReference("Trash Data Remaining", StringType, nullable = false)())
    } else if (showStats) {
      Seq(
        AttributeReference("Size Freed", StringType, nullable = false)())
      // actual clean files operation
    } else {
      Seq.empty
    }
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    // if insert overwrite in progress, do not allow delete segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      if ((carbonTable.isMV && !isInternalCleanCall) || !carbonTable.isMV) {
        throw new ConcurrentOperationException(carbonTable, "insert overwrite", "clean file")
      } else if (carbonTable.isMV && isInternalCleanCall) {
        LOGGER.info(s"Clean files not allowed on table: {${carbonTable.getTableName}}")
        return Seq.empty
      }
    }
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    var sizeCleaned : Long = 0
    if (isDryRun) {
      val result = DataTrashManager.cleanFilesDryRunOperation(
        carbonTable,
        options.getOrElse("force", "false").toBoolean,
        options.getOrElse("stale_inprogress", "false").toBoolean,
        showStats)
      Seq(Row(ByteUtil.convertByteToReadable(result._1), ByteUtil
          .convertByteToReadable(result._2)))
    } else {
      val preEvent = CleanFilesPreEvent(carbonTable, sparkSession)
      val postEvent = CleanFilesPostEvent(carbonTable, sparkSession, options)
      withEvents(preEvent, postEvent) {
         sizeCleaned = DataTrashManager.cleanGarbageData(
          carbonTable,
          options.getOrElse("force", "false").toBoolean,
          options.getOrElse("stale_inprogress", "false").toBoolean,
          showStats,
          CarbonFilters.getPartitions(Seq.empty[Expression], sparkSession, carbonTable))
      }
      if (showStats) {
        Seq(Row(ByteUtil.convertByteToReadable(sizeCleaned)))
      } else {
        Seq.empty
      }
    }
  }

  override protected def opName: String = "CLEAN FILES"
}
