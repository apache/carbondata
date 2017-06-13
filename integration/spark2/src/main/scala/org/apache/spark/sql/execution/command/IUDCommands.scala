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

package org.apache.spark.sql.execution.command

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.storage.StorageLevel

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, LockUsage}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, DeleteDeltaBlockDetails, SegmentUpdateDetails, TupleIdEnum}
import org.apache.carbondata.core.mutate.data.RowCountDetailsVO
import org.apache.carbondata.core.statusmanager.{SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl
import org.apache.carbondata.processing.exception.MultipleMatchingException
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CarbonDataMergerUtilResult, CompactionType}
import org.apache.carbondata.spark.DeleteDelataResultImpl
import org.apache.carbondata.spark.load.FailureCauses
import org.apache.carbondata.spark.util.QueryPlanUtil


/**
 * IUD update delete and compaction framework.
 *
 */

private[sql] case class ProjectForDeleteCommand(
     plan: LogicalPlan,
     identifier: Seq[String],
     timestamp: String) extends RunnableCommand {

  val LOG = LogServiceFactory.getLogService(this.getClass.getName)
  var horizontalCompactionFailed = false

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dataFrame = Dataset.ofRows(sparkSession, plan)
//    dataFrame.show(truncate = false)
//    dataFrame.collect().foreach(println)
    val dataRdd = dataFrame.rdd

    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(deleteExecution.getTableIdentifier(identifier))(sparkSession).
      asInstanceOf[CarbonRelation]
    val carbonTable = relation.tableMeta.carbonTable
    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK)
    var lockStatus = false
    try {
      lockStatus = metadataLock.lockWithRetries()
      LOG.audit(s" Delete data request has been received " +
                s"for ${ relation.databaseName }.${ relation.tableName }.")
      if (lockStatus) {
        LOG.info("Successfully able to get the table metadata file lock")
      }
      else {
        throw new Exception("Table is locked for deletion. Please try after some time")
      }
      val tablePath = CarbonStorePath.getCarbonTablePath(
        carbonTable.getStorePath,
        carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier)
      var executorErrors = new ExecutionErrors(FailureCauses.NONE, "")

        // handle the clean up of IUD.
        CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

          if (deleteExecution
            .deleteDeltaExecution(identifier, sparkSession, dataRdd, timestamp, relation,
              false, executorErrors)) {
            // call IUD Compaction.
            IUDCommon.tryHorizontalCompaction(sparkSession, relation, isUpdateOperation = false)
          }
    } catch {
      case e: HorizontalCompactionException =>
          LOG.error("Delete operation passed. Exception in Horizontal Compaction." +
              " Please check logs. " + e.getMessage)
          CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, e.compactionTimeStamp.toString)

      case e: Exception =>
        LOG.error("Exception in Delete data operation " + e.getMessage)
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

private[sql] case class ProjectForUpdateCommand(
    plan: LogicalPlan, tableIdentifier: Seq[String]) extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(ProjectForUpdateCommand.getClass.getName)

  override def run(sparkSession: SparkSession): Seq[Row] = {


   //  sqlContext.sparkContext.setLocalProperty(org.apache.spark.sql.execution.SQLExecution
    //  .EXECUTION_ID_KEY, null)
    // DataFrame(sqlContext, plan).show(truncate = false)
    // return Seq.empty


    val res = plan find {
      case relation: LogicalRelation if (relation.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) =>
        true
      case _ => false
    }

    if (!res.isDefined) {
      return Seq.empty
    }
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(deleteExecution.getTableIdentifier(tableIdentifier))(sparkSession).
      asInstanceOf[CarbonRelation]
//    val relation = CarbonEnv.get.carbonMetastore
//      .lookupRelation1(deleteExecution.getTableIdentifier(tableIdentifier))(sqlContext).
//      asInstanceOf[CarbonRelation]
    val carbonTable = relation.tableMeta.carbonTable
    val metadataLock = CarbonLockFactory
      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier,
        LockUsage.METADATA_LOCK)
    var lockStatus = false
    // get the current time stamp which should be same for delete and update.
    val currentTime = CarbonUpdateUtil.readCurrentTime
//    var dataFrame: DataFrame = null
    var dataSet: DataFrame = null
    val isPersistEnabledUserValue = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.isPersistEnabled,
        CarbonCommonConstants.defaultValueIsPersistEnabled)
   var isPersistEnabled = CarbonCommonConstants.defaultValueIsPersistEnabled.toBoolean
    if (isPersistEnabledUserValue.equalsIgnoreCase("false")) {
      isPersistEnabled = false
    }
    else if (isPersistEnabledUserValue.equalsIgnoreCase("true")) {
      isPersistEnabled = true
    }
    try {
      lockStatus = metadataLock.lockWithRetries()
      if (lockStatus) {
        logInfo("Successfully able to get the table metadata file lock")
      }
      else {
        throw new Exception("Table is locked for updation. Please try after some time")
      }
      val tablePath = CarbonStorePath.getCarbonTablePath(
        carbonTable.getStorePath,
        carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier)
        // Get RDD.

      dataSet = if (isPersistEnabled) {
          Dataset.ofRows(sparkSession, plan).persist(StorageLevel.MEMORY_AND_DISK)
//          DataFrame(sqlContext, plan)
//            .persist(StorageLevel.MEMORY_AND_DISK)
        }
        else {
          Dataset.ofRows(sparkSession, plan)
//          DataFrame(sqlContext, plan)
        }
        var executionErrors = new ExecutionErrors(FailureCauses.NONE, "")


        // handle the clean up of IUD.
        CarbonUpdateUtil.cleanUpDeltaFiles(carbonTable, false)

        // do delete operation.
        deleteExecution.deleteDeltaExecution(tableIdentifier, sparkSession, dataSet.rdd,
          currentTime + "",
        relation, isUpdateOperation = true, executionErrors)

        if(executionErrors.failureCauses != FailureCauses.NONE) {
          throw new Exception(executionErrors.errorMsg)
        }

        // do update operation.
        UpdateExecution.performUpdate(dataSet, tableIdentifier, plan,
          sparkSession, currentTime, executionErrors)

        if(executionErrors.failureCauses != FailureCauses.NONE) {
          throw new Exception(executionErrors.errorMsg)
        }

        // Do IUD Compaction.
        IUDCommon.tryHorizontalCompaction(sparkSession, relation, isUpdateOperation = true)
    }

    catch {
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
}

object IUDCommon {

  val LOG = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * The method does horizontal compaction. After Update and Delete completion
   * tryHorizontal compaction will be called. In case this method is called after
   * Update statement then Update Compaction followed by Delete Compaction will be
   * processed whereas for tryHorizontalCompaction called after Delete statement
   * then only Delete Compaction will be processed.
    *
    * @param sparkSession
   * @param carbonRelation
   * @param isUpdateOperation
   */
  def tryHorizontalCompaction(sparkSession: SparkSession,
      carbonRelation: CarbonRelation,
      isUpdateOperation: Boolean): Unit = {

    var ishorizontalCompaction = CarbonDataMergerUtil.isHorizontalCompactionEnabled()

    if (ishorizontalCompaction == false) {
      return
    }

    var compactionTypeIUD = CompactionType.IUD_UPDDEL_DELTA_COMPACTION
    val carbonTable = carbonRelation.tableMeta.carbonTable
    val (db, table) = (carbonTable.getDatabaseName, carbonTable.getFactTableName)
    val absTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val updateTimeStamp = System.currentTimeMillis()
    // To make sure that update and delete timestamps are not same,
    // required to commit to status metadata and cleanup
    val deleteTimeStamp = updateTimeStamp + 1

    // get the valid segments
    var segLists = CarbonDataMergerUtil.getValidSegmentList(absTableIdentifier)

    if (segLists == null || segLists.size() == 0) {
      return
    }

    // Should avoid reading Table Status file from Disk every time. Better to load it
    // in-memory at the starting and pass it along the routines. The constructor of
    // SegmentUpdateStatusManager reads the Table Status File and Table Update Status
    // file and save the content in segmentDetails and updateDetails respectively.
    val segmentUpdateStatusManager: SegmentUpdateStatusManager = new SegmentUpdateStatusManager(
      absTableIdentifier)

    if (isUpdateOperation == true) {

      // This is only update operation, perform only update compaction.
      compactionTypeIUD = CompactionType.IUD_UPDDEL_DELTA_COMPACTION
      performUpdateDeltaCompaction(sparkSession,
        compactionTypeIUD,
        carbonTable,
        absTableIdentifier,
        segmentUpdateStatusManager,
        updateTimeStamp,
        segLists)
    }

    // After Update Compaction perform delete compaction
    compactionTypeIUD = CompactionType.IUD_DELETE_DELTA_COMPACTION
    segLists = CarbonDataMergerUtil.getValidSegmentList(absTableIdentifier)
    if (segLists == null || segLists.size() == 0) {
      return
    }

    // Delete Compaction
    performDeleteDeltaCompaction(sparkSession,
      compactionTypeIUD,
      carbonTable,
      absTableIdentifier,
      segmentUpdateStatusManager,
      deleteTimeStamp,
      segLists)
  }

  /**
   * Update Delta Horizontal Compaction.
    *
    * @param sparkSession
   * @param compactionTypeIUD
   * @param carbonTable
   * @param absTableIdentifier
   * @param segLists
   */
  private def performUpdateDeltaCompaction(sparkSession: SparkSession,
      compactionTypeIUD: CompactionType,
      carbonTable: CarbonTable,
      absTableIdentifier: AbsoluteTableIdentifier,
      segmentUpdateStatusManager: SegmentUpdateStatusManager,
      factTimeStamp: Long,
      segLists: util.List[String]): Unit = {
    val db = carbonTable.getDatabaseName
    val table = carbonTable.getFactTableName
    // get the valid segments qualified for update compaction.
    val validSegList = CarbonDataMergerUtil.getSegListIUDCompactionQualified(segLists,
      absTableIdentifier,
      segmentUpdateStatusManager,
      compactionTypeIUD)

    if (validSegList.size() == 0) {
      return
    }

    LOG.info(s"Horizontal Update Compaction operation started for [${db}.${table}].")
    LOG.audit(s"Horizontal Update Compaction operation started for [${db}.${table}].")

    try {
      // Update Compaction.
      val altertablemodel = AlterTableModel(Option(carbonTable.getDatabaseName),
        carbonTable.getFactTableName,
        Some(segmentUpdateStatusManager),
        CompactionType.IUD_UPDDEL_DELTA_COMPACTION.toString,
        Some(factTimeStamp),
        "")

      AlterTableCompaction(altertablemodel).run(sparkSession)
    }
    catch {
      case e: Exception =>
        val msg = if (null != e.getMessage) {
          e.getMessage
        } else {
          "Please check logs for more info"
        }
        throw new HorizontalCompactionException(
          s"Horizontal Update Compaction Failed for [${ db }.${ table }]. " + msg, factTimeStamp)
    }
    LOG.info(s"Horizontal Update Compaction operation completed for [${ db }.${ table }].")
    LOG.audit(s"Horizontal Update Compaction operation completed for [${ db }.${ table }].")
  }

  /**
   * Delete Delta Horizontal Compaction.
    *
    * @param sparkSession
   * @param compactionTypeIUD
   * @param carbonTable
   * @param absTableIdentifier
   * @param segLists
   */
  private def performDeleteDeltaCompaction(sparkSession: SparkSession,
      compactionTypeIUD: CompactionType,
      carbonTable: CarbonTable,
      absTableIdentifier: AbsoluteTableIdentifier,
      segmentUpdateStatusManager: SegmentUpdateStatusManager,
      factTimeStamp: Long,
      segLists: util.List[String]): Unit = {

    val db = carbonTable.getDatabaseName
    val table = carbonTable.getFactTableName
    val deletedBlocksList = CarbonDataMergerUtil.getSegListIUDCompactionQualified(segLists,
      absTableIdentifier,
      segmentUpdateStatusManager,
      compactionTypeIUD)

    if (deletedBlocksList.size() == 0) {
      return
    }

    LOG.info(s"Horizontal Delete Compaction operation started for [${db}.${table}].")
    LOG.audit(s"Horizontal Delete Compaction operation started for [${db}.${table}].")

    try {

      // Delete Compaction RDD
      val rdd1 = sparkSession.sparkContext
        .parallelize(deletedBlocksList.asScala.toSeq, deletedBlocksList.size())

      val timestamp = factTimeStamp
      val updateStatusDetails = segmentUpdateStatusManager.getUpdateStatusDetails
      val result = rdd1.mapPartitions(iter =>
        new Iterator[Seq[CarbonDataMergerUtilResult]] {
          override def hasNext: Boolean = iter.hasNext

          override def next(): Seq[CarbonDataMergerUtilResult] = {
            val segmentAndBlocks = iter.next
            val segment = segmentAndBlocks.substring(0, segmentAndBlocks.lastIndexOf("/"))
            val blockName = segmentAndBlocks
              .substring(segmentAndBlocks.lastIndexOf("/") + 1, segmentAndBlocks.length)

            val result = CarbonDataMergerUtil.compactBlockDeleteDeltaFiles(segment, blockName,
              absTableIdentifier,
              updateStatusDetails,
              timestamp)

            result.asScala.toList

          }
        }).collect

      val resultList = ListBuffer[CarbonDataMergerUtilResult]()
      result.foreach(x => {
        x.foreach(y => {
          resultList += y
        })
      })

      val updateStatus = CarbonDataMergerUtil.updateStatusFile(resultList.toList.asJava,
        carbonTable,
        timestamp.toString,
        segmentUpdateStatusManager)
      if (updateStatus == false) {
        LOG.audit(s"Delete Compaction data operation is failed for [${db}.${table}].")
        LOG.error("Delete Compaction data operation is failed.")
        throw new HorizontalCompactionException(
          s"Horizontal Delete Compaction Failed for [${db}.${table}] ." +
          s" Please check logs for more info.", factTimeStamp)
      }
      else {
        LOG.info(s"Horizontal Delete Compaction operation completed for [${db}.${table}].")
        LOG.audit(s"Horizontal Delete Compaction operation completed for [${db}.${table}].")
      }
    }
    catch {
      case e: Exception =>
        val msg = if (null != e.getMessage) {
          e.getMessage
        } else {
          "Please check logs for more info"
        }
        throw new HorizontalCompactionException(
          s"Horizontal Delete Compaction Failed for [${ db }.${ table }]. " + msg, factTimeStamp)
    }
  }
}

class HorizontalCompactionException(
    message: String,
    // required for cleanup
    val compactionTimeStamp: Long) extends RuntimeException(message) {
}

object deleteExecution {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def getTableIdentifier(tableIdentifier: Seq[String]): TableIdentifier = {
    if (tableIdentifier.size > 1) {
      TableIdentifier(tableIdentifier(1), Some(tableIdentifier(0)))
    } else {
      TableIdentifier(tableIdentifier(0), None)
    }
  }

  def deleteDeltaExecution(identifier: Seq[String],
                           sparkSession: SparkSession,
                           dataRdd: RDD[Row],
                           timestamp: String, relation: CarbonRelation, isUpdateOperation: Boolean,
                           executorErrors: ExecutionErrors): Boolean = {

    var res: Array[List[(String, (SegmentUpdateDetails, ExecutionErrors))]] = null
    val tableName = getTableIdentifier(identifier).table
    val database = getDB.getDatabaseName(getTableIdentifier(identifier).database, sparkSession)
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(deleteExecution.getTableIdentifier(identifier))(sparkSession).
      asInstanceOf[CarbonRelation]

    val storeLocation = relation.tableMeta.storePath
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new
        AbsoluteTableIdentifier(storeLocation,
          relation.tableMeta.carbonTableIdentifier)
    var tablePath = CarbonStorePath
      .getCarbonTablePath(storeLocation,
        absoluteTableIdentifier.getCarbonTableIdentifier())
    var tableUpdateStatusPath = tablePath.getTableUpdateStatusFilePath
    val totalSegments =
      SegmentStatusManager.readLoadMetadata(tablePath.getMetadataDirectoryPath).length
    var factPath = tablePath.getFactDir

    var carbonTable = relation.tableMeta.carbonTable
    var deleteStatus = true
    val deleteRdd = if (isUpdateOperation) {
      val schema =
        org.apache.spark.sql.types.StructType(Seq(org.apache.spark.sql.types.StructField(
          CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
          org.apache.spark.sql.types.StringType)))
      val rdd = dataRdd
        .map(row => Row(row.get(row.fieldIndex(
          CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))))
      sparkSession.createDataFrame(rdd, schema).rdd
      // sqlContext.createDataFrame(rdd, schema).rdd
    } else {
      dataRdd
    }

    val (carbonInputFormat, job) =
      QueryPlanUtil.createCarbonInputFormat(absoluteTableIdentifier)

    val keyRdd = deleteRdd.map({ row =>
      val tupleId: String = row
        .getString(row.fieldIndex(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
      val key = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleId)
      (key, row)
    }).groupByKey()

    // if no loads are present then no need to do anything.
    if (keyRdd.partitions.size == 0) {
      return true
    }

    var blockMappingVO = carbonInputFormat.getBlockRowCount(job, absoluteTableIdentifier)
    val segmentUpdateStatusMngr = new SegmentUpdateStatusManager(absoluteTableIdentifier)
    CarbonUpdateUtil
      .createBlockDetailsMap(blockMappingVO, segmentUpdateStatusMngr)

    val rowContRdd =
      sparkSession.sparkContext.parallelize(
        blockMappingVO.getCompleteBlockRowDetailVO.asScala.toSeq,
          keyRdd.partitions.length)

//    val rowContRdd = sqlContext.sparkContext
//      .parallelize(blockMappingVO.getCompleteBlockRowDetailVO.asScala.toSeq,
//        keyRdd.partitions.size)

    val rdd = rowContRdd.join(keyRdd)

    // rdd.collect().foreach(println)

    res = rdd.mapPartitionsWithIndex(
      (index: Int, records: Iterator[((String), (RowCountDetailsVO, Iterable[Row]))]) =>
        Iterator[List[(String, (SegmentUpdateDetails, ExecutionErrors))]] {

          var result = List[(String, (SegmentUpdateDetails, ExecutionErrors))]()
          while (records.hasNext) {
            val ((key), (rowCountDetailsVO, groupedRows)) = records.next
            result = result ++
              deleteDeltaFunc(index,
                key,
                groupedRows.toIterator,
                timestamp,
                rowCountDetailsVO)

          }
          result
        }
    ).collect()

    // if no loads are present then no need to do anything.
    if (res.isEmpty) {
      return true
    }

    // update new status file
    checkAndUpdateStatusFiles

    // all or none : update status file, only if complete delete opeartion is successfull.
    def checkAndUpdateStatusFiles: Unit = {
      val blockUpdateDetailsList = new util.ArrayList[SegmentUpdateDetails]()
      val segmentDetails = new util.HashSet[String]()
      res.foreach(resultOfSeg => resultOfSeg.foreach(
        resultOfBlock => {
          if (resultOfBlock._1.equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)) {
            blockUpdateDetailsList.add(resultOfBlock._2._1)
            segmentDetails.add(resultOfBlock._2._1.getSegmentName)
            // if this block is invalid then decrement block count in map.
            if (CarbonUpdateUtil.isBlockInvalid(resultOfBlock._2._1.getStatus)) {
              CarbonUpdateUtil.decrementDeletedBlockCount(resultOfBlock._2._1,
                blockMappingVO.getSegmentNumberOfBlockMapping)
            }
          }
          else {
            deleteStatus = false
            // In case of failure , clean all related delete delta files
            CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
            LOGGER.audit(s"Delete data operation is failed for ${ database }.${ tableName }")
            val errorMsg =
              "Delete data operation is failed due to failure in creating delete delta file for " +
                "segment : " + resultOfBlock._2._1.getSegmentName + " block : " +
                resultOfBlock._2._1.getBlockName
            executorErrors.failureCauses = resultOfBlock._2._2.failureCauses
            executorErrors.errorMsg = resultOfBlock._2._2.errorMsg

            if (executorErrors.failureCauses == FailureCauses.NONE) {
              executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
              executorErrors.errorMsg = errorMsg
            }
            LOGGER.error(errorMsg)
            return
          }
        }
      )
      )

      val listOfSegmentToBeMarkedDeleted = CarbonUpdateUtil
        .getListOfSegmentsToMarkDeleted(blockMappingVO.getSegmentNumberOfBlockMapping)



      // this is delete flow so no need of putting timestamp in the status file.
      if (CarbonUpdateUtil
        .updateSegmentStatus(blockUpdateDetailsList, carbonTable, timestamp, false) &&
        CarbonUpdateUtil
          .updateTableMetadataStatus(segmentDetails,
            carbonTable,
            timestamp,
            !isUpdateOperation,
            listOfSegmentToBeMarkedDeleted)
      ) {
        LOGGER.info(s"Delete data operation is successful for ${ database }.${ tableName }")
        LOGGER.audit(s"Delete data operation is successful for ${ database }.${ tableName }")
      }
      else {
        // In case of failure , clean all related delete delta files
        CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)

        val errorMessage = "Delete data operation is failed due to failure " +
          "in table status updation."
        LOGGER.audit(s"Delete data operation is failed for ${ database }.${ tableName }")
        LOGGER.error("Delete data operation is failed due to failure in table status updation.")
        executorErrors.failureCauses = FailureCauses.STATUS_FILE_UPDATION_FAILURE
        executorErrors.errorMsg = errorMessage
        // throw new Exception(errorMessage)
      }
    }

    def deleteDeltaFunc(index: Int,
                        key: String,
                        iter: Iterator[Row],
                        timestamp: String,
                        rowCountDetailsVO: RowCountDetailsVO):
    Iterator[(String, (SegmentUpdateDetails, ExecutionErrors))] = {

      val result = new DeleteDelataResultImpl()
      var deleteStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
      // here key = segment/blockName
      val blockName = CarbonUpdateUtil
        .getBlockName(
          CarbonTablePath.addDataPartPrefix(key.split(CarbonCommonConstants.FILE_SEPARATOR)(1)))
      val segmentId = key.split(CarbonCommonConstants.FILE_SEPARATOR)(0)
      var deleteDeltaBlockDetails: DeleteDeltaBlockDetails = new DeleteDeltaBlockDetails(blockName)
      val resultIter = new Iterator[(String, (SegmentUpdateDetails, ExecutionErrors))] {
        val segmentUpdateDetails = new SegmentUpdateDetails()
        var TID = ""
        var countOfRows = 0
        try {
          while (iter.hasNext) {
            val oneRow = iter.next
            TID = oneRow
              .get(oneRow.fieldIndex(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID)).toString
            val offset = CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.OFFSET)
            val blockletId = CarbonUpdateUtil
              .getRequiredFieldFromTID(TID, TupleIdEnum.BLOCKLET_ID)
            val IsValidOffset = deleteDeltaBlockDetails.addBlocklet(blockletId, offset)
            // stop delete operation
            if(!IsValidOffset) {
              executorErrors.failureCauses = FailureCauses.MULTIPLE_INPUT_ROWS_MATCHING
              executorErrors.errorMsg = "Multiple input rows matched for same row."
              throw new MultipleMatchingException("Multiple input rows matched for same row.")
            }
            countOfRows = countOfRows + 1
          }

          val blockPath = CarbonUpdateUtil.getTableBlockPath(TID, factPath)
          val completeBlockName = CarbonTablePath
            .addDataPartPrefix(CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.BLOCK_ID) +
              CarbonCommonConstants.FACT_FILE_EXT)
          val deleteDeletaPath = CarbonUpdateUtil
            .getDeleteDeltaFilePath(blockPath, blockName, timestamp)
          val carbonDeleteWriter = new CarbonDeleteDeltaWriterImpl(deleteDeletaPath,
            FileFactory.getFileType(deleteDeletaPath))



          segmentUpdateDetails.setBlockName(blockName)
          segmentUpdateDetails.setActualBlockName(completeBlockName)
          segmentUpdateDetails.setSegmentName(segmentId)
          segmentUpdateDetails.setDeleteDeltaEndTimestamp(timestamp)
          segmentUpdateDetails.setDeleteDeltaStartTimestamp(timestamp)

          val alreadyDeletedRows: Long = rowCountDetailsVO.getDeletedRowsInBlock
          val totalDeletedRows: Long = alreadyDeletedRows + countOfRows
          segmentUpdateDetails.setDeletedRowsInBlock(totalDeletedRows.toString)
          if (totalDeletedRows == rowCountDetailsVO.getTotalNumberOfRows) {
            segmentUpdateDetails.setStatus(CarbonCommonConstants.MARKED_FOR_DELETE)
          }
          else {
            // write the delta file
            carbonDeleteWriter.write(deleteDeltaBlockDetails)
          }

          deleteStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
        } catch {
          case e : MultipleMatchingException =>
            LOGGER.audit(e.getMessage)
            LOGGER.error(e.getMessage)
          // dont throw exception here.
          case e: Exception =>
            val errorMsg = s"Delete data operation is failed for ${ database }.${ tableName }."
            LOGGER.audit(errorMsg)
            LOGGER.error(errorMsg + e.getMessage)
            throw e
        }


        var finished = false

        override def hasNext: Boolean = {
          if (!finished) {
            finished = true
            finished
          }
          else {
            !finished
          }
        }

        override def next(): (String, (SegmentUpdateDetails, ExecutionErrors)) = {
          finished = true
          result.getKey(deleteStatus, (segmentUpdateDetails, executorErrors))
        }
      }
      resultIter
    }
    true
  }
}



object UpdateExecution {

  def performUpdate(
         dataFrame: Dataset[Row],
         tableIdentifier: Seq[String],
         plan: LogicalPlan,
         sparkSession: SparkSession,
         currentTime: Long,
         executorErrors: ExecutionErrors): Unit = {

    def isDestinationRelation(relation: CarbonDatasourceHadoopRelation): Boolean = {

      val tableName = relation.absIdentifier.getCarbonTableIdentifier.getTableName
      val dbName = relation.absIdentifier.getCarbonTableIdentifier.getDatabaseName
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

    LoadTable(
      Some(carbonRelation.absIdentifier.getCarbonTableIdentifier.getDatabaseName),
      carbonRelation.absIdentifier.getCarbonTableIdentifier.getTableName,
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
