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

  /**
   * This method triggers the merge action based calling merge handler
   * @param carbonTable target carbon table
   * @param factTimestamp the timestamp used for update and delete actions
   * @param executorErrors executor errors returned from the operations like update and delete
   * @param update RDD[ROW] which contains the rows to update or delete
   * @return the segment list and the metadata details of the segments updated or deleted
   */
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

  /**
   * This method updates the segment status after update or delete operation
   * @param targetCarbonTable target carbon table
   * @param factTimeStamp timestamp to update in the status which is used in update/delete operation
   * @param tuple contains the segment list and the metadata details of the segments updated/deleted
   */
  def updateSegmentStatusAfterUpdateOrDelete(targetCarbonTable: CarbonTable,
      factTimeStamp: Long,
      tuple: (util.List[SegmentUpdateDetails], Seq[Segment])): Unit = {
    if (!CarbonUpdateUtil.updateSegmentStatus(tuple._1, targetCarbonTable,
      factTimeStamp.toString, false, false)) {
      LOGGER.error("writing of update status file failed")
      throw new CarbonMergeDataSetException("writing of update status file failed")
    }
  }

  /**
   * This methods inserts the data to target carbon table.
   * @param targetCarbonTable target carbon table to insert
   * @param header            header of the data to be inserted
   * @param updateTableModel  updated model if any for insert
   * @param dataFrame         datframe to write into target carbon table.
   * @return the segmentID created afterthis insert operation.
   */
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

  /**
   * This method is to update the status only for delete operation.
   * @param targetCarbonTable target carbon table
   * @param factTimestamp timestamp to update in the status which is used in update/delete operation
   * @return whether update status is successful or not
   */
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
