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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.LongType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.indexserver.IndexServer
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

  var isDeleteRepeat = false
  var deleteSegments: String = null
  var repeatedSegments: String = null

  def getDataRdd(
      sparkSession: SparkSession,
      plan: LogicalPlan): RDD[Row] = {
    val newPlan = if (isDeleteRepeat) {
      Dataset.ofRows(sparkSession, plan).queryExecution.optimizedPlan transform {
        case Join(left, right, j, c) =>
          val newLeft = left transform {
            case lr: LogicalRelation
              if (lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) =>
              val r = lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].copy()
              r.segmentsForDelete = deleteSegments
              LogicalRelation(r)
            case other => other
          }
          val newRight = right transform {
            case lr: LogicalRelation
              if (lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) =>
              val r = lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].copy()
              r.segmentsForDelete = repeatedSegments
              LogicalRelation(r)
            case other => other
          }
          Join(newLeft, newRight, j, c)
        case other => other
      }
    } else {
      plan
    }
    val dataFrame = Dataset.ofRows(sparkSession, newPlan)
    dataFrame.rdd
  }

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Deleted Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("plan" -> plan.simpleString))
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "loading", "data delete")
    }

    if (!carbonTable.canAllow(carbonTable, TableOperation.DELETE)) {
      throw new MalformedCarbonCommandException(
        "delete operation is not supported for index datamap")
    }

    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, plan)
    val dataRdd = getDataRdd(sparkSession, plan)

    // trigger event for Delete from table
    val operationContext = new OperationContext
    val deleteFromTablePreEvent: DeleteFromTablePreEvent =
      DeleteFromTablePreEvent(sparkSession, carbonTable)
    OperationListenerBus.getInstance.fireEvent(deleteFromTablePreEvent, operationContext)

    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    val compactionLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)
    val updateLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.UPDATE_LOCK)
    var lockStatus = false
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        if (!compactionLock.lockWithRetries(3, 3)) {
          throw new ConcurrentOperationException(carbonTable, "compaction", "delete")
        }
        if (!updateLock.lockWithRetries(3, 3)) {
          throw new ConcurrentOperationException(carbonTable, "update/delete", "delete")
        }
        LOGGER.info("Successfully able to get the table metadata file lock")
      } else {
        throw new Exception("Table is locked for deletion. Please try after some time")
      }
      val executorErrors = ExecutionErrors(FailureCauses.NONE, "")

      // handle the clean up of IUD.
      CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

      val (deletedSegments, deletedRowCount) = DeleteExecution.deleteDeltaExecution(
        databaseNameOp,
        tableName,
        sparkSession,
        dataRdd,
        timestamp,
        isUpdateOperation = false,
        executorErrors)

      // Check for any failures occured during delete delta execution
      if (executorErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executorErrors.errorMsg)
      }

      // call IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(sparkSession, carbonTable,
        isUpdateOperation = false)

      DeleteExecution.clearDistributedSegmentCache(carbonTable, deletedSegments)(sparkSession)

      val allDataMapSchemas = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
        .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                 !dataMapSchema.isIndexDataMap).asJava
      if (!allDataMapSchemas.isEmpty) {
        DataMapStatusManager.truncateDataMap(allDataMapSchemas)
      }

      // trigger post event for Delete from table
      val deleteFromTablePostEvent: DeleteFromTablePostEvent =
        DeleteFromTablePostEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance.fireEvent(deleteFromTablePostEvent, operationContext)
      Seq(Row(deletedRowCount))
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error("Delete operation passed. Exception in Horizontal Compaction." +
                     " Please check logs. " + e.getMessage)
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)
        Seq(Row(0L))

      case e: Exception =>
        LOGGER.error("Exception in Delete data operation " + e.getMessage, e)
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
      updateLock.unlock()
      compactionLock.unlock()
    }
  }

  override protected def opName: String = "DELETE DATA"
}
