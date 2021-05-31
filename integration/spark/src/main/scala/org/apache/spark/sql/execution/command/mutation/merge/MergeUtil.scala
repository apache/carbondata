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
package org.apache.spark.sql.execution.command.mutation.merge

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.command.{ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand
import org.apache.spark.sql.execution.command.mutation.DeleteExecution

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, SegmentUpdateDetails}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext


object MergeUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def triggerAction(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      factTimestamp: Long,
      executorErrors: ExecutionErrors,
      update: RDD[Row]): (util.List[SegmentUpdateDetails], Seq[Segment]) = {
    val tuple1 = DeleteExecution.deleteDeltaExecutionInternal(Some(carbonTable.getDatabaseName),
      carbonTable.getTableName,
      sparkSession, update,
      factTimestamp.toString,
      isUpdateOperation = true, executorErrors, Some(0))
    MutationActionFactory.checkErrors(executorErrors)
    val tupleProcessed1 = DeleteExecution.processSegments(executorErrors, tuple1._1, carbonTable,
      factTimestamp.toString, tuple1._2)
    MutationActionFactory.checkErrors(executorErrors)
    tupleProcessed1
  }

  def updateSegmentStatusAfterUpdateOrDelete(targetCarbonTable: CarbonTable,
      factTimeStamp: Long,
      tuple: (util.List[SegmentUpdateDetails], Seq[Segment])): Unit = {
    if (!CarbonUpdateUtil.updateSegmentStatus(tuple._1, targetCarbonTable,
      factTimeStamp.toString, false, false)) {
      LOGGER.error("writing of update status file failed")
      throw new CarbonMergeDataSetException("writing of update status file failed")
    }
  }

  def insertDataToTargetTable(sparkSession: SparkSession,
      targetCarbonTable: CarbonTable,
      header: String,
      updateTableModel: Option[UpdateTableModel],
      dataFrame: DataFrame): Seq[Row] = {
    CarbonInsertIntoCommand(databaseNameOp = Some(targetCarbonTable.getDatabaseName),
      tableName = targetCarbonTable.getTableName,
      options = Map("fileheader" -> header),
      isOverwriteTable = false,
      dataFrame.queryExecution.logical,
      targetCarbonTable.getTableInfo,
      Map.empty,
      Map.empty,
      new OperationContext,
      updateTableModel
    ).run(sparkSession)
  }

  def updateStatusIfJustDeleteOperation(targetCarbonTable: CarbonTable,
      factTimestamp: Long): Boolean = {
    val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
      .getTableStatusFilePath(targetCarbonTable.getTablePath))
    CarbonUpdateUtil.updateTableMetadataStatus(loadMetaDataDetails.map(loadMetadataDetail =>
      new Segment(loadMetadataDetail.getMergedLoadName,
        loadMetadataDetail.getSegmentFile)).toSet.asJava,
      targetCarbonTable,
      factTimestamp.toString,
      true,
      true, new util.ArrayList[Segment]())
  }
}
