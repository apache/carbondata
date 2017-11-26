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

package org.apache.spark.sql.execution.command.mutation

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.LoadTableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, UpdateTablePostEvent, UpdateTablePreEvent}
import org.apache.carbondata.processing.loading.FailureCauses

private[sql] case class ProjectForUpdateCommand(
    plan: LogicalPlan, tableIdentifier: Seq[String])
  extends RunnableCommand with DataProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(ProjectForUpdateCommand.getClass.getName)
    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, plan)
    val res = plan find {
      case relation: LogicalRelation if relation.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        true
      case _ => false
    }

    if (res.isEmpty) {
      return Seq.empty
    }
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(DeleteExecution.getTableIdentifier(tableIdentifier))(sparkSession).
      asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable

    // trigger event for Update table
    val operationContext = new OperationContext
    val updateTablePreEvent: UpdateTablePreEvent =
      UpdateTablePreEvent(carbonTable)
    OperationListenerBus.getInstance.fireEvent(updateTablePreEvent, operationContext)

    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    var lockStatus = false
    // get the current time stamp which should be same for delete and update.
    val currentTime = CarbonUpdateUtil.readCurrentTime
    //    var dataFrame: DataFrame = null
    var dataSet: DataFrame = null
    val isPersistEnabled = CarbonProperties.getInstance.isPersistUpdateDataset
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        throw new Exception("Table is locked for updation. Please try after some time")
      }
      // Get RDD.

      dataSet = if (isPersistEnabled) {
        Dataset.ofRows(sparkSession, plan).persist(StorageLevel.fromString(
          CarbonProperties.getInstance.getUpdateDatasetStorageLevel()))
      }
      else {
        Dataset.ofRows(sparkSession, plan)
      }
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")


      // handle the clean up of IUD.
      CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

      // do delete operation.
      DeleteExecution.deleteDeltaExecution(tableIdentifier, sparkSession, dataSet.rdd,
        currentTime + "", isUpdateOperation = true, executionErrors)

      if(executionErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executionErrors.errorMsg)
      }

      // do update operation.
      performUpdate(dataSet, tableIdentifier, plan, sparkSession, currentTime, executionErrors)

      if(executionErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executionErrors.errorMsg)
      }

      // Do IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(sparkSession, relation, isUpdateOperation = true)

      // trigger event for Update table
      val updateTablePostEvent: UpdateTablePostEvent =
        UpdateTablePostEvent(carbonTable)
      OperationListenerBus.getInstance.fireEvent(updateTablePostEvent, operationContext)
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error(
          "Update operation passed. Exception in Horizontal Compaction. Please check logs." + e)
        // In case of failure , clean all related delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)

      case e: Exception =>
        LOGGER.error("Exception in update operation" + e)
        // ****** start clean up.
        // In case of failure , clean all related delete delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, currentTime + "")

        // *****end clean up.
        if (null != e.getMessage) {
          sys.error("Update operation failed. " + e.getMessage)
        }
        if (null != e.getCause && null != e.getCause.getMessage) {
          sys.error("Update operation failed. " + e.getCause.getMessage)
        }
        sys.error("Update operation failed. please check logs.")
    }
    finally {
      if (null != dataSet && isPersistEnabled) {
        dataSet.unpersist()
      }
      if (lockStatus) {
        CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK)
      }
    }
    Seq.empty
  }

  private def performUpdate(
      dataFrame: Dataset[Row],
      tableIdentifier: Seq[String],
      plan: LogicalPlan,
      sparkSession: SparkSession,
      currentTime: Long,
      executorErrors: ExecutionErrors): Unit = {

    def isDestinationRelation(relation: CarbonDatasourceHadoopRelation): Boolean = {

      val tableName = relation.identifier.getCarbonTableIdentifier.getTableName
      val dbName = relation.identifier.getCarbonTableIdentifier.getDatabaseName
      (tableIdentifier.size > 1 &&
       tableIdentifier(0) == dbName &&
       tableIdentifier(1) == tableName) ||
      (tableIdentifier(0) == tableName)
    }
    def getHeader(relation: CarbonDatasourceHadoopRelation, plan: LogicalPlan): String = {
      var header = ""
      var found = false

      plan match {
        case Project(pList, _) if (!found) =>
          found = true
          header = pList
            .filter(field => !field.name
              .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
            .map(col => if (col.name.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION)) {
              col.name
                .substring(0, col.name.lastIndexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION))
            }
            else {
              col.name
            }).mkString(",")
      }
      header
    }
    val ex = dataFrame.queryExecution.analyzed
    val res = ex find {
      case relation: LogicalRelation
        if relation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           isDestinationRelation(relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]) =>
        true
      case _ => false
    }
    val carbonRelation: CarbonDatasourceHadoopRelation = res match {
      case Some(relation: LogicalRelation) =>
        relation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
      case _ => sys.error("")
    }

    val updateTableModel = UpdateTableModel(true, currentTime, executorErrors)

    val header = getHeader(carbonRelation, plan)

    LoadTableCommand(
      Some(carbonRelation.identifier.getCarbonTableIdentifier.getDatabaseName),
      carbonRelation.identifier.getCarbonTableIdentifier.getTableName,
      null,
      Seq(),
      Map(("fileheader" -> header)),
      false,
      null,
      Some(dataFrame),
      Some(updateTableModel)).run(sparkSession)

    executorErrors.errorMsg = updateTableModel.executorErrors.errorMsg
    executorErrors.failureCauses = updateTableModel.executorErrors.failureCauses

    Seq.empty

  }
}
