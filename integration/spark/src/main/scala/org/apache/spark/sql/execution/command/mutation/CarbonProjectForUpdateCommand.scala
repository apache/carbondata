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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.MixedFormatHandler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, UpdateTablePostEvent, UpdateTablePreEvent}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.view.MVManagerInSpark

private[sql] case class CarbonProjectForUpdateCommand(
    plan: LogicalPlan,
    databaseNameOp: Option[String],
    tableName: String,
    columns: List[String])
  extends DataCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Updated Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    var updatedRowCount = 0L
    IUDCommonUtil.checkIfSegmentListIsSet(sparkSession, plan)
    val res = plan find {
      case relation: LogicalRelation if relation.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        true
      case _ => false
    }

    if (res.isEmpty) {
      return Array(Row(updatedRowCount)).toSeq
    }
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("plan" -> plan.prettyJson))
    // Do not allow spatial index and its source columns to be updated.
    AlterTableUtil.validateColumnsWithSpatialIndexProperties(carbonTable, columns)
    columns.foreach { col =>
      val dataType = carbonTable.getColumnByName(col).getColumnSchema.getDataType
      if (dataType.isComplexType) {
        throw new UnsupportedOperationException("Unsupported operation on Complex data type")
      }

    }
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (SegmentStatusManager.isLoadInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "loading", "data update")
    }

    if (!carbonTable.canAllow(carbonTable, TableOperation.UPDATE)) {
      throw new MalformedCarbonCommandException(
        "update operation is not supported for index")
    }

    // Block the update operation for non carbon formats
    if (MixedFormatHandler.otherFormatSegmentsExist(carbonTable.getMetadataPath)) {
      throw new MalformedCarbonCommandException(
        s"Unsupported update operation on table containing mixed format segments")
    }

    // trigger event for Update table
    val operationContext = new OperationContext
    val updateTablePreEvent: UpdateTablePreEvent =
      UpdateTablePreEvent(sparkSession, carbonTable)
    operationContext.setProperty("isLoadOrCompaction", false)
    OperationListenerBus.getInstance.fireEvent(updateTablePreEvent, operationContext)
    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
        LockUsage.METADATA_LOCK)
    val compactionLock = CarbonLockFactory.getCarbonLockObj(carbonTable
      .getAbsoluteTableIdentifier, LockUsage.COMPACTION_LOCK)
    val updateLock = CarbonLockFactory.getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
      LockUsage.UPDATE_LOCK)
    var lockStatus = false
    // get the current time stamp which should be same for delete and update.
    val currentTime = CarbonUpdateUtil.readCurrentTime
    //    var dataFrame: DataFrame = null
    var dataSet: DataFrame = null
    val isPersistEnabled = CarbonProperties.getInstance.isPersistUpdateDataset
    var hasHorizontalCompactionException = false
    var hasUpdateException = false
    var fileTimestamp = ""
    var updateTableModel: UpdateTableModel = null
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        throw new Exception("Table is locked for update. Please try after some time")
      }

      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      if (updateLock.lockWithRetries()) {
        if (compactionLock.lockWithRetries()) {
          // Get RDD.
          dataSet = if (isPersistEnabled) {
            Dataset.ofRows(sparkSession, plan).persist(StorageLevel.fromString(
              CarbonProperties.getInstance.getUpdateDatasetStorageLevel()))
          }
          else {
            Dataset.ofRows(sparkSession, plan)
          }
          if (CarbonProperties.isUniqueValueCheckEnabled) {
            // If more than one value present for the update key, should fail the update
            val ds = dataSet.select(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)
              .groupBy(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)
              .count()
              .select("count")
              .filter(col("count") > lit(1))
              .limit(1)
              .collect()
            // tupleId represents the source rows that are going to get replaced.
            // If same tupleId appeared more than once means key has more than one value to replace.
            // which is undefined behavior.
            if (ds.length > 0 && ds(0).getLong(0) > 1) {
              throw new UnsupportedOperationException(
                " update cannot be supported for 1 to N mapping, as more than one value present " +
                "for the update key")
            }
          }

          // do delete operation.
          val (segmentsToBeDeleted, updatedRowCountTemp) = DeleteExecution.deleteDeltaExecution(
            databaseNameOp,
            tableName,
            sparkSession,
            dataSet.rdd,
            currentTime + "",
            isUpdateOperation = true,
            executionErrors)

          if (executionErrors.failureCauses != FailureCauses.NONE) {
            throw new Exception(executionErrors.errorMsg)
          }

          updatedRowCount = updatedRowCountTemp
          updateTableModel =
            UpdateTableModel(true, currentTime, executionErrors, segmentsToBeDeleted, Option.empty)
          // do update operation.
          performUpdate(dataSet,
            databaseNameOp,
            tableName,
            plan,
            sparkSession,
            updateTableModel,
            executionErrors)

          // pre-priming for update command
          DeleteExecution.reloadDistributedSegmentCache(carbonTable,
            segmentsToBeDeleted, operationContext)(sparkSession)

        } else {
          throw new ConcurrentOperationException(carbonTable, "compaction", "update")
        }
      } else {
        throw new ConcurrentOperationException(carbonTable, "update/delete", "update")
      }
      if (executionErrors.failureCauses != FailureCauses.NONE) {
        throw new Exception(executionErrors.errorMsg)
      }

      // Do IUD Compaction.
      HorizontalCompaction.tryHorizontalCompaction(
        sparkSession, carbonTable)

      // Truncate materialized views on the current table.
      val viewManager = MVManagerInSpark.get(sparkSession)
      val viewSchemas = viewManager.getSchemasOnTable(carbonTable)
      if (!viewSchemas.isEmpty) {
        viewManager.onTruncate(viewSchemas)
      }

      // trigger event for Update table
      val updateTablePostEvent: UpdateTablePostEvent =
        UpdateTablePostEvent(sparkSession, carbonTable)
      OperationListenerBus.getInstance.fireEvent(updateTablePostEvent, operationContext)
    } catch {
      case e: HorizontalCompactionException =>
        LOGGER.error(
          "Update operation passed. Exception in Horizontal Compaction. Please check logs." + e)
        // In case of failure , clean all related delta files
        fileTimestamp = e.compactionTimeStamp.toString
        hasHorizontalCompactionException = true
      case e: Exception =>
        LOGGER.error("Exception in update operation", e)
        fileTimestamp = currentTime + ""
        hasUpdateException = true
        if (null != e.getMessage) {
          sys.error("Update operation failed. " + e.getMessage)
        }
        if (null != e.getCause && null != e.getCause.getMessage) {
          sys.error("Update operation failed. " + e.getCause.getMessage)
        }
        sys.error("Update operation failed. please check logs.")
    } finally {
      // In case of failure, clean new inserted segment,
      // change the status of new segment to 'mark for delete' from 'success'
      if (hasUpdateException && null != updateTableModel
        && updateTableModel.insertedSegment.isDefined) {
        CarbonLoaderUtil.updateTableStatusInCaseOfFailure(updateTableModel.insertedSegment.get,
          carbonTable, SegmentStatus.SUCCESS)
      }

      if (updateLock.unlock()) {
        LOGGER.info(s"updateLock unlocked successfully after update $tableName")
      } else {
        LOGGER.error(s"Unable to unlock updateLock for table $tableName after table update");
      }

      if (compactionLock.unlock()) {
        LOGGER.info(s"compactionLock unlocked successfully after update $tableName")
      } else {
        LOGGER.error(s"Unable to unlock compactionLock for " +
          s"table $tableName after update");
      }

      if (lockStatus) {
        CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK)
      }

      if (null != dataSet && isPersistEnabled) {
        try {
          dataSet.unpersist()
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in update $tableName" + e.getMessage, e)
        }
      }

      // In case of failure, clean all related delete delta files.
      if (hasHorizontalCompactionException || hasUpdateException) {
        // In case of failure , clean all related delete delta files
        // When the table has too many segemnts, it will take a long time.
        // So moving it to the end and it is outside of locking.
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, fileTimestamp)
      }
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
  }

  override protected def opName: String = "UPDATE DATA"
}
