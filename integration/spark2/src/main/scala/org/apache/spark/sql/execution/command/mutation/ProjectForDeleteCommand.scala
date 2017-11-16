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

import org.apache.spark.sql.{CarbonEnv, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.FailureCauses
/**
 * IUD update delete and compaction framework.
 *
 */
private[sql] case class ProjectForDeleteCommand(
    plan: LogicalPlan,
    identifier: Seq[String],
    timestamp: String) extends RunnableCommand with DataProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
      IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, plan)
    val dataFrame = Dataset.ofRows(sparkSession, plan)
    //    dataFrame.show(truncate = false)
    //    dataFrame.collect().foreach(println)
    val dataRdd = dataFrame.rdd

    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(DeleteExecution.getTableIdentifier(identifier))(sparkSession).
      asInstanceOf[CarbonRelation]
    val carbonTable = relation.carbonTable

    // trigger event for Delete from table
    val operationContext = new OperationContext
    val deleteFromTablePreEvent: DeleteFromTablePreEvent =
      DeleteFromTablePreEvent(carbonTable)
    OperationListenerBus.getInstance.fireEvent(deleteFromTablePreEvent, operationContext)

    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    var lockStatus = false
    try {
      lockStatus = metadataLock.lockWithRetries()
      LOGGER.audit(s" Delete data request has been received " +
                   s"for ${ relation.databaseName }.${ relation.tableName }.")
      if (lockStatus) {
        LOGGER.info("Successfully able to get the table metadata file lock")
      } else {
        throw new Exception("Table is locked for deletion. Please try after some time")
      }
      val executorErrors = ExecutionErrors(FailureCauses.NONE, "")

      // handle the clean up of IUD.
      CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

      if (DeleteExecution.deleteDeltaExecution(identifier, sparkSession, dataRdd, timestamp,
        isUpdateOperation = false, executorErrors)) {
        // call IUD Compaction.
        HorizontalCompaction.tryHorizontalCompaction(sparkSession, relation,
          isUpdateOperation = false)

        // trigger post event for Delete from table
        val deleteFromTablePostEvent: DeleteFromTablePostEvent =
          DeleteFromTablePostEvent(carbonTable)
        OperationListenerBus.getInstance.fireEvent(deleteFromTablePostEvent, operationContext)
      }
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
