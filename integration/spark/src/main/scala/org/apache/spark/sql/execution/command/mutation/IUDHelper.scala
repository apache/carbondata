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
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.MixedFormatHandler
import org.apache.spark.sql.types.ArrayType

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, OperationContext, OperationListenerBus, UpdateTablePostEvent, UpdateTablePreEvent}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.view.MVManagerInSpark

/**
 * IUD update delete and compaction framework.
 *
 */

object IUDHelper {

   def processDeleteOp(plan: LogicalPlan, carbonTable: CarbonTable,
                     timeStamp: String, sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    // Block the delete operation for non carbon formats
    if (MixedFormatHandler.otherFormatSegmentsExist(carbonTable.getMetadataPath)) {
      throw new MalformedCarbonCommandException(
        s"Unsupported delete operation on table containing mixed format segments")
    }

    if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "loading", "data delete")
    }

    if (!carbonTable.canAllow(carbonTable, TableOperation.DELETE)) {
      throw new MalformedCarbonCommandException("delete operation is not supported for index")
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
    val compactionLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)
    val updateLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.UPDATE_LOCK)
    var lockStatus = false
    var hasException = false
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        if (!compactionLock.lockWithRetries()) {
          throw new ConcurrentOperationException(carbonTable, "compaction", "delete")
        }
        if (!updateLock.lockWithRetries()) {
          throw new ConcurrentOperationException(carbonTable, "update/delete", "delete")
        }
        LOGGER.info("Successfully able to get the table metadata file lock")
      } else {
        throw new Exception("Table is locked for deletion. Please try after some time")
      }
      val executorErrors = ExecutionErrors(FailureCauses.NONE, "")

      val (deletedSegments, deletedRowCount) = DeleteExecution.deleteDeltaExecution(
        Option(carbonTable.getDatabaseName),
        carbonTable.getTableName,
        sparkSession,
        dataRdd,
        timeStamp,
        isUpdateOperation = false,
        executorErrors)

      // Check for any failures occurred during delete delta execution
      if (executorErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executorErrors.errorMsg)
      }

      // call IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(sparkSession, carbonTable)

      // Truncate materialized views on the current table.
      val viewManager = MVManagerInSpark.get(sparkSession)
      val viewSchemas = viewManager.getSchemasOnTable(carbonTable)
      if (!viewSchemas.isEmpty) {
        viewManager.onTruncate(viewSchemas)
      }

      // pre-priming for delete command
      DeleteExecution.reloadDistributedSegmentCache(carbonTable,
        deletedSegments, operationContext)(sparkSession)

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
        hasException = true
        Seq(Row(0L))

      case e: Exception =>
        LOGGER.error("Exception in Delete data operation " + e.getMessage, e)
        // ****** start clean up.
        // In case of failure , clean all related delete delta files
        hasException = true

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
      val tableName = carbonTable.getTableName
      if (updateLock.unlock()) {
        LOGGER.info(s"updateLock unlocked successfully after delete operation $tableName")
      } else {
        LOGGER.error(s"Unable to unlock updateLock for table $tableName after delete operation");
      }

      if (compactionLock.unlock()) {
        LOGGER.info(s"compactionLock unlocked successfully after delete operation $tableName")
      } else {
        LOGGER.error(s"Unable to unlock compactionLock for " +
          s"table $tableName after delete operation");
      }

      if (hasException) {
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timeStamp)
      }
    }
  }

  def performUpdate(
                             dataFrame: Dataset[Row],
                             databaseNameOp: Option[String],
                             tableName: String,
                             plan: LogicalPlan,
                             sparkSession: SparkSession,
                             updateTableModel: UpdateTableModel,
                             executorErrors: ExecutionErrors): Unit = {

    def isDestinationRelation(relation: CarbonDatasourceHadoopRelation): Boolean = {
      val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
      (databaseNameOp.isDefined &&
        databaseNameOp.get == dbName &&
        tableName == relation.identifier.getCarbonTableIdentifier.getTableName) ||
        (tableName == relation.identifier.getCarbonTableIdentifier.getTableName)
    }

    // from the dataFrame schema iterate through all the column to be updated and
    // check for the data type, if the data type is complex then throw exception
    def checkForUnsupportedDataType(dataFrame: DataFrame): Unit = {
      dataFrame.schema.foreach(col => {
        // the new column to be updated will be appended with "-updatedColumn" suffix
        if (col.name.endsWith(CarbonCommonConstants.UPDATED_COL_EXTENSION) &&
          col.dataType.isInstanceOf[ArrayType]) {
          throw new UnsupportedOperationException("Unsupported data type: Array")
        }
      })
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

    // check for the data type of the new value to be updated
    checkForUnsupportedDataType(dataFrame)
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

    val header = getHeader(carbonRelation, plan)
    val fields = dataFrame.schema.fields
    val otherFields = CarbonScalaUtil.getAllFieldsWithoutTupleIdField(fields)
    val dataFrameWithOutTupleId = dataFrame.select(otherFields: _*)

    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonRelation.identifier.getCarbonTableIdentifier.getDatabaseName),
      tableName = carbonRelation.identifier.getCarbonTableIdentifier.getTableName,
      options = Map(("fileheader" -> header)),
      isOverwriteTable = false,
      dataFrame = dataFrameWithOutTupleId,
      updateModel = Some(updateTableModel)).process(sparkSession)

    executorErrors.errorMsg = updateTableModel.executorErrors.errorMsg
    executorErrors.failureCauses = updateTableModel.executorErrors.failureCauses
  }

}
