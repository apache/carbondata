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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.types.LongType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.FailureCauses

/**
 * IUD update delete and compaction framework.
 *
 */
case class CarbonProjectForDeleteCommand(
    logicPlan: LogicalPlan,
    databaseNameOp: Option[String],
    tableName: String,
    timestamp: String)
  extends DataCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Deleted Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    IUDCommonUtil.checkPreconditionsForDelete(sparkSession, logicPlan, carbonTable)

    setAuditTable(carbonTable)
    setAuditInfo(Map("plan" -> logicPlan.simpleString))

    // Step1: trigger PreDelete event for table
    val operationContext = new OperationContext
    val deleteFromTablePreEvent: DeleteFromTablePreEvent =
      DeleteFromTablePreEvent(sparkSession, carbonTable)
    OperationListenerBus.getInstance.fireEvent(deleteFromTablePreEvent, operationContext)

    // Step2. acquire locks
    val locksToBeAcquired =
      List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK, LockUsage.UPDATE_LOCK)
    val acquiredLocks =
      CarbonLockUtil.acquireLocks(carbonTable, locksToBeAcquired.asJava)

    var hasDeleteException = false
    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    var updatedSegmentList: util.Set[String] = new util.HashSet[String]()
    var deletedSegmentList: util.Set[String] = new util.HashSet[String]()
    var deletedRowCount = 0L

    try {
      // Step3 get deleted data
      val dataFrame = Dataset.ofRows(sparkSession, logicPlan)

      // Step4.2 do delete operation.CarbonDataRDDFactory.scala
      val (blockUpdateDetailsList, updatedSegmentListTmp,
          deletedSegmentListTmp, deletedRowCountTmp) =
        DeleteExecution.deleteDeltaExecution(
        carbonTable,
        sparkSession,
          dataFrame.rdd,
        timestamp,
        isUpdateOperation = false,
        executorErrors)
      deletedRowCount = deletedRowCountTmp
      updatedSegmentList = updatedSegmentListTmp
      deletedSegmentList = deletedSegmentListTmp
      if (deletedRowCount == 0) return Seq(Row(0L))

      // Step4.3 write updatetablestatus and tablestatus
      DeleteExecution.checkAndUpdateStatusFiles(executorErrors,
        carbonTable, timestamp + "", false, null,
        blockUpdateDetailsList, updatedSegmentList, deletedSegmentList)

      // Step5.1 Delta Compaction
      IUDCommonUtil.tryHorizontalCompaction(sparkSession,
        carbonTable, updatedSegmentList.asScala.toSet)

      // Step5.2 Refresh MV and SI
      val deleteFromTablePostEvent: DeleteFromTablePostEvent =
        DeleteFromTablePostEvent(sparkSession, carbonTable)
      IUDCommonUtil.refreshMVandIndex(sparkSession, carbonTable,
        operationContext, deleteFromTablePostEvent)
    } catch {
      case e: Exception =>
        LOGGER.error("Exception in Delete data operation " + e.getMessage, e)
        // ****** start clean up.
        // In case of failure , clean all related delete delta files
        hasDeleteException = true

        // clean up. Null check is required as for executor error some times message is null
        if (null != e.getMessage) {
          sys.error("Delete data operation is failed. " + e.getMessage)
        }
        else {
          sys.error("Delete data operation is failed. Please check logs.")
        }
    } finally {
      // release the locks
      CarbonLockUtil.releaseLocks(acquiredLocks.asInstanceOf[java.util.List[ICarbonLock]])

      if (hasDeleteException) {
        // In case of failure , clean all related delete delta files
        // When the table has too many segemnts, it will take a long time.
        // So moving it to the end and it is outside of locking.
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
      }
    }

    Seq(Row(deletedRowCount))
  }

  override protected def opName: String = "DELETE DATA"
}
