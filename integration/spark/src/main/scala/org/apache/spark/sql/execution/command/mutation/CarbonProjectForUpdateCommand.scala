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

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.{OperationContext, OperationListenerBus, UpdateTablePostEvent, UpdateTablePreEvent}
import org.apache.carbondata.processing.loading.FailureCauses

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
    setAuditInfo(Map("plan" -> plan.simpleString))
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
        "update operation is not supported for index datamap")
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
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        throw new Exception("Table is locked for updation. Please try after some time")
      }

      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      if (updateLock.lockWithRetries(3, 3)) {
        if (compactionLock.lockWithRetries(3, 3)) {
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
          // handle the clean up of IUD.
          CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

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
          // do update operation.
          performUpdate(dataSet,
            databaseNameOp,
            tableName,
            plan,
            sparkSession,
            currentTime,
            executionErrors,
            segmentsToBeDeleted)

          // prepriming for update command
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
        sparkSession, carbonTable, isUpdateOperation = true)

      val allDataMapSchemas = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
        .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                 !dataMapSchema.isIndexDataMap).asJava
      if (!allDataMapSchemas.isEmpty) {
        DataMapStatusManager.truncateDataMap(allDataMapSchemas)
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
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)

      case e: Exception =>
        LOGGER.error("Exception in update operation", e)
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
    } finally {
      if (null != dataSet && isPersistEnabled) {
        dataSet.unpersist()
      }
      updateLock.unlock()
      compactionLock.unlock()
      if (lockStatus) {
        CarbonLockUtil.fileUnlock(metadataLock, LockUsage.METADATA_LOCK)
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
      currentTime: Long,
      executorErrors: ExecutionErrors,
      deletedSegments: Seq[Segment]): Unit = {

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

    val updateTableModel = UpdateTableModel(true, currentTime, executorErrors, deletedSegments)

    val header = getHeader(carbonRelation, plan)

    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonRelation.identifier.getCarbonTableIdentifier.getDatabaseName),
      tableName = carbonRelation.identifier.getCarbonTableIdentifier.getTableName,
      options = Map(("fileheader" -> header)),
      isOverwriteTable = false,
      dataFrame = dataFrame,
      updateModel = Some(updateTableModel)).process(sparkSession)

    executorErrors.errorMsg = updateTableModel.executorErrors.errorMsg
    executorErrors.failureCauses = updateTableModel.executorErrors.failureCauses
  }

  override protected def opName: String = "UPDATE DATA"
}
