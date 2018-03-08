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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.common.exceptions.ConcurrentOperationException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.FailureCauses

/**
 * IUD update delete and compaction framework.
 *
 */
private[sql] case class CarbonProjectForDeleteCommand(
    plan: LogicalPlan,
    databaseNameOp: Option[String],
    tableName: String,
    timestamp: String)
  extends DataCommand {

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "loading", "data delete")
    }

    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, plan)
    val dataFrame = Dataset.ofRows(sparkSession, plan)
    val dataRdd = dataFrame.rdd

    // trigger event for Delete from table
    val operationContext = new OperationContext
    val deleteFromTablePreEvent: DeleteFromTablePreEvent =
      DeleteFromTablePreEvent(sparkSession, carbonTable)
    OperationListenerBus.getInstance.fireEvent(deleteFromTablePreEvent, operationContext)

    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    var lockStatus = false
    try {
      lockStatus = metadataLock.lockWithRetries()
      LOGGER.audit(s" Delete data request has been received " +
                   s"for ${carbonTable.getDatabaseName}.${carbonTable.getTableName}.")
      if (lockStatus) {
        LOGGER.info("Successfully able to get the table metadata file lock")
      } else {
        throw new Exception("Table is locked for deletion. Please try after some time")
      }
      val executorErrors = ExecutionErrors(FailureCauses.NONE, "")

      // handle the clean up of IUD.
      CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

      DeleteExecution.deleteDeltaExecution(
        databaseNameOp,
        tableName,
        sparkSession,
        dataRdd,
        timestamp,
        isUpdateOperation = false,
        executorErrors)
      // call IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(sparkSession, carbonTable,
        isUpdateOperation = false)


      if (executorErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executorErrors.errorMsg)
      }

      // trigger post event for Delete from table
      val deleteFromTablePostEvent: DeleteFromTablePostEvent =
        DeleteFromTablePostEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance.fireEvent(deleteFromTablePostEvent, operationContext)
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error("Delete operation passed. Exception in Horizontal Compaction." +
                     " Please check logs. " + e.getMessage)
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)

      case e: Exception =>
        LOGGER.error(e, "Exception in Delete data operation " + e.getMessage)
        // ****** start clean up.
        // In case of failure , clean all related delete delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)

        // clean up. Null check is required as for executor error some times message is null
        if (null != e.getMessage) {
          sys.error("Delete data operation is failed. " + e.getMessage)
        }
        else {
          sys.error("Delete data operation is failed. Please check logs.")
        }
    } finally {
      if (lockStatus) {
        CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK)
      }
    }
    Seq.empty
  }
}
