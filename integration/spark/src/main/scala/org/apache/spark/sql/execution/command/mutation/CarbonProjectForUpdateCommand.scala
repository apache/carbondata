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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.locks.{CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, UpdateTablePostEvent, UpdateTablePreEvent}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

case class CarbonProjectForUpdateCommand(
    logicPlan: LogicalPlan,
    databaseNameOp: Option[String],
    tableName: String,
    columns: List[String])
  extends DataCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Updated Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    IUDCommonUtil.checkPreconditionsForUpdate(sparkSession, logicPlan, carbonTable, columns)

    setAuditTable(carbonTable)
    setAuditInfo(Map("plan" -> logicPlan.simpleString))

    // Step1: trigger PreUpdate event for table
    val operationContext = new OperationContext
    val updateTablePreEvent: UpdateTablePreEvent = UpdateTablePreEvent(sparkSession, carbonTable)
    OperationListenerBus.getInstance.fireEvent(updateTablePreEvent, operationContext)

    // Step2. acquire locks
    val locksToBeAcquired =
      List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK, LockUsage.UPDATE_LOCK)
    val acquiredLocks =
      CarbonLockUtil.acquireLocks(carbonTable, locksToBeAcquired.asJava)

    // Initialize the variables
    var updatedRowCount = 0L
    var dataFrame: DataFrame = null
    var hasUpdateException = false
    val deltaDeltaFileTimestamp = CarbonUpdateUtil.readCurrentTime.toString
    var updateTableModel: UpdateTableModel = null
    val executionErrors = ExecutionErrors(FailureCauses.NONE, "")
    val isPersistEnabled = CarbonProperties.getInstance.isPersistUpdateDataset
    var updatedSegmentList: util.Set[String] = new util.HashSet[String]()
    var deletedSegmentList: util.Set[String] = new util.HashSet[String]()

    try {
      // Step3 get updated data
      dataFrame = if (isPersistEnabled) {
        Dataset.ofRows(sparkSession, logicPlan).persist(StorageLevel.fromString(
          CarbonProperties.getInstance.getUpdateDatasetStorageLevel()))
      } else {
        Dataset.ofRows(sparkSession, logicPlan)
      }

      // Step4.1 check unique value if needed
      if (CarbonProperties.isUniqueValueCheckEnabled) {
        // If more than one value present for the update key, should fail the update
        IUDCommonUtil.uniqueValueCheck(dataFrame)
      }

      // Step4.3 do delete operation.
      val (blockUpdateDetailsList, updatedSegmentListTmp,
          deleteSegmentListTmp, updatedRowCountTmp) =
        DeleteExecution.deleteDeltaExecution(
        carbonTable, sparkSession,
        dataFrame.rdd,
        deltaDeltaFileTimestamp,
        isUpdateOperation = true,
        executionErrors,
        Some(dataFrame.schema.fields.length - 1))
      updatedSegmentList = updatedSegmentListTmp
      deletedSegmentList = deleteSegmentListTmp
      updatedRowCount = updatedRowCountTmp
      if (updatedRowCount == 0) return Seq(Row(0L))

      // Step4.4 do insert operation.
      updateTableModel = UpdateTableModel(true, deltaDeltaFileTimestamp.toLong,
        executionErrors, Seq.empty, Option.empty, Option.empty)
      performUpdate(dataFrame,
        databaseNameOp,
        tableName,
        logicPlan,
        sparkSession,
        updateTableModel,
        executionErrors)

      // Step4.5 write updatetablestatus and tablestatus
      DeleteExecution.checkAndUpdateStatusFiles(executionErrors, carbonTable,
        deltaDeltaFileTimestamp.toString, true,
        updateTableModel, blockUpdateDetailsList, updatedSegmentList, deletedSegmentList)

      // Step5.1 Delta Compaction
      IUDCommonUtil.tryHorizontalCompaction(sparkSession,
        carbonTable, updatedSegmentList.asScala.toSet)

      // Step5.2 Refresh MV
      val updateTablePostEvent: UpdateTablePostEvent =
        UpdateTablePostEvent(sparkSession, carbonTable)
      IUDCommonUtil.refreshMVandIndex(sparkSession, carbonTable,
        operationContext, updateTablePostEvent)


      // If there are already index and data files are present for old update operation, then the
      // cache will be loaded for those files during current update, but once after horizontal
      // compaction is finished, new compacted files are generated, so the segments inside cache are
      // now invalid, so clear the cache of invalid segment after horizontal compaction.
      IndexStoreManager.getInstance()
        .clearInvalidSegments(carbonTable, deletedSegmentList.asScala.toList.asJava)
    } catch {
      case e: Exception =>
        LOGGER.error("Exception in update operation", e)
        hasUpdateException = true
        if (null != e.getMessage) {
          sys.error("Update operation failed. " + e.getMessage)
        }
        if (null != e.getCause && null != e.getCause.getMessage) {
          sys.error("Update operation failed. " + e.getCause.getMessage)
        }
        sys.error("Update operation failed. please check logs.")
    } finally {
      // release the locks
      CarbonLockUtil.releaseLocks(acquiredLocks.asInstanceOf[java.util.List[ICarbonLock]])

      if (null != dataFrame && isPersistEnabled) {
        try {
          dataFrame.unpersist()
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in update $tableName" + e.getMessage, e)
        }
      }

      if (hasUpdateException) {
        // In case of failure , clean all related delete delta files
        // When the table has too many segemnts, it will take a long time.
        // So moving it to the end and it is outside of locking.
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, deltaDeltaFileTimestamp)
      }
    }

    if (hasUpdateException) {
      return Seq(Row(0L))
    }

    // Step5.2 Minor Compaction
    try {
      if (null != updateTableModel && updateTableModel.addedLoadDetail.isDefined) {
        // Trigger auto compaction
        CarbonDataRDDFactory.handleSegmentMerging(
          sparkSession.sqlContext,
          updateTableModel.carbonLoadModel.get,
          carbonTable,
          new util.ArrayList[String](),
          operationContext)
      }
    } catch {
      case e: Exception =>
        LOGGER.error("Compaction in Update operation failed. Please check logs." + e)
    }
    Seq(Row(updatedRowCount))
  }

  private def performUpdate(
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

    if (executorErrors.failureCauses != FailureCauses.NONE) {
      throw new Exception(executorErrors.errorMsg)
    }
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

  override protected def opName: String = "UPDATE DATA"
}
