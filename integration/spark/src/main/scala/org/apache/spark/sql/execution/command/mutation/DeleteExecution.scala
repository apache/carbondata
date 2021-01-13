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
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, DeleteDeltaBlockDetails, SegmentUpdateDetails, TupleIdEnum}
import org.apache.carbondata.core.mutate.data.{BlockMappingVO, RowCountDetailsVO}
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.writer.CarbonDeleteDeltaWriterImpl
import org.apache.carbondata.events.{IndexServerLoadEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.processing.exception.MultipleMatchingException
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.spark.DeleteDeltaResultImpl
import org.apache.carbondata.spark.util.CarbonSparkUtil

object DeleteExecution {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def deleteDeltaExecution(
      carbonTable: CarbonTable,
      sparkSession: SparkSession,
      dataRdd: RDD[Row],
      timestamp: String,
      isUpdateOperation: Boolean,
      executorErrors: ExecutionErrors,
      tupleId: Option[Int] = None):
  (util.List[SegmentUpdateDetails], util.HashSet[String], util.HashSet[String], Long) = {

    val (res, blockMappingVO) = deleteDeltaExecutionInternal(carbonTable,
      sparkSession, dataRdd, timestamp, isUpdateOperation, executorErrors, tupleId)

    var operatedRowCount = 0L
    // if no loads are present then no need to do anything.
    if (res.flatten.isEmpty) {
      return (new util.ArrayList[SegmentUpdateDetails](),
        new util.HashSet[String](), new util.HashSet[String](), 0L)
    }

    val (blockUpdateDetailsList, updatedSegmentList, deletedSegmentList) =
      DeleteExecution.processSegments(executorErrors, res, blockMappingVO)

    // Check for any failures occurred during delete delta execution
    if (executorErrors.failureCauses == FailureCauses.NONE) {
      operatedRowCount = res.flatten.map(_._2._3).sum
    } else {
      throw new Exception(executorErrors.errorMsg)
    }
    (blockUpdateDetailsList, updatedSegmentList, deletedSegmentList, operatedRowCount)
  }

  /**
   * generate the delete delta files in each segment as per the RDD.
   * @return it gives the segments which needs to be deleted.
   */
  def deleteDeltaExecutionInternal(
      carbonTable: CarbonTable,
      sparkSession: SparkSession,
      dataRdd: RDD[Row],
      timestamp: String,
      isUpdateOperation: Boolean,
      executorErrors: ExecutionErrors,
      tupleId: Option[Int] = None):
  (Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]], BlockMappingVO) = {

    var res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]] = null
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val tablePath = absoluteTableIdentifier.getTablePath

    val deletedRdd = if (isUpdateOperation && tupleId.isDefined) {
      dataRdd.map(row => Row(row.get(tupleId.get)))
    } else {
      dataRdd
    }

    val keyRdd = deletedRdd.map { row =>
      val tupleId: String = row.getString(0)
      val key = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleId,
        carbonTable.isHivePartitionTable)
      (key, row)
    }.aggregateByKey(List[Row]())(
      (acc, x) => x :: acc,
      (acc1, acc2) => acc1 ::: acc2
    )

    val (carbonInputFormat, job) = createCarbonInputFormat(absoluteTableIdentifier)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    val blockMappingVO =
      carbonInputFormat.getBlockRowCount(
        job,
        carbonTable,
        CarbonFilters.getPartitions(
          Seq.empty,
          sparkSession,
          carbonTable).map(_.asJava).orNull, true)
    val segmentUpdateStatusMngr = new SegmentUpdateStatusManager(carbonTable)
    CarbonUpdateUtil
      .createBlockDetailsMap(blockMappingVO, segmentUpdateStatusMngr)
    val blockRowDetailVOMap = blockMappingVO.getCompleteBlockRowDetailVO

    val isStandardTable = CarbonUtil.isStandardCarbonTable(carbonTable)

    val conf = SparkSQLUtil
      .broadCastHadoopConf(sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())
    val blockDetails = blockMappingVO.getBlockToSegmentMapping
    val externalSegmentPathMap = SegmentStatusManager.readTableStatusFile(
      CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
      .filter(detail => StringUtils.isNotEmpty(detail.getPath))
      .map(f => (f.getLoadName, f.getPath)).toMap

    res = keyRdd.mapPartitionsWithIndex(
      (index: Int, records: Iterator[(String, (Iterable[Row]))]) =>
        Iterator[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]] {
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
          var result = List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]()
          while (records.hasNext) {
            val (blockId, groupedRows) = records.next
            val segmentId = blockDetails.get(blockId)
            val externalSegmentPath = externalSegmentPathMap.get(segmentId)
            result = result ++
                     deleteDeltaFunc(index,
                       blockId,
                       groupedRows.toIterator,
                       timestamp,
                       blockRowDetailVOMap.get(blockId),
                       isStandardTable,
                       carbonTable,
                       segmentId,
                       externalSegmentPath)
          }
          result
        }).collect()

    def deleteDeltaFunc(index: Int,
        key: String,
        iter: Iterator[Row],
        timestamp: String,
        rowCountDetailsVO: RowCountDetailsVO,
        isStandardTable: Boolean,
        carbonTable: CarbonTable,
        segmentId: String,
        externalSegmentPath: Option[String]
    ): Iterator[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))] = {

      val result = new DeleteDeltaResultImpl()
      var deleteStatus = SegmentStatus.LOAD_FAILURE
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
      // here key = segment/blockName
      var blockName = if (carbonTable.isHivePartitionTable) {
        key
      } else {
        key.split(CarbonCommonConstants.FILE_SEPARATOR)(1)
      }
      blockName = blockName.replace(CarbonCommonConstants.UNDERSCORE, CarbonTablePath.BATCH_PREFIX)
      blockName = CarbonUpdateUtil.getBlockName(CarbonTablePath.addDataPartPrefix(blockName))
      val deleteDeltaBlockDetails: DeleteDeltaBlockDetails = new DeleteDeltaBlockDetails(blockName)
      val resultIter =
        new Iterator[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))] {
        val segmentUpdateDetails = new SegmentUpdateDetails()
        var TID = ""
        var countOfRows = 0
        try {
          while (iter.hasNext) {
            val oneRow = iter.next
            TID = oneRow.get(0).toString
            val (offset, blockletId, pageId) = if (carbonTable.isHivePartitionTable) {
              (CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                TupleIdEnum.OFFSET.getTupleIdIndex),
                CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                  TupleIdEnum.BLOCKLET_ID.getTupleIdIndex),
                Integer.parseInt(CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                  TupleIdEnum.PAGE_ID.getTupleIdIndex)))
            } else if (TID.contains("#/") && externalSegmentPath.isDefined) {
              // this is in case of the external segment, where the tuple id has external path with#
              (CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.EXTERNAL_OFFSET),
                CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.EXTERNAL_BLOCKLET_ID),
                Integer.parseInt(CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                  TupleIdEnum.EXTERNAL_PAGE_ID)))
            } else {
              (CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.OFFSET),
                CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.BLOCKLET_ID),
                Integer.parseInt(CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                  TupleIdEnum.PAGE_ID)))
            }
            val IsValidOffset = deleteDeltaBlockDetails.addBlocklet(blockletId, offset, pageId)
            // stop delete operation
            if(!IsValidOffset) {
              executorErrors.failureCauses = FailureCauses.MULTIPLE_INPUT_ROWS_MATCHING
              executorErrors.errorMsg = "Multiple input rows matched for same row."
              throw new MultipleMatchingException("Multiple input rows matched for same row.")
            }
            countOfRows = countOfRows + 1
          }

          val blockPath =
            if (externalSegmentPath.isDefined) {
              externalSegmentPath.get
            } else {
              CarbonUpdateUtil.getTableBlockPath(TID,
                tablePath,
                isStandardTable,
                carbonTable.isHivePartitionTable)
            }

          // get the compressor name
          var columnCompressor: String = carbonTable.getTableInfo
            .getFactTable
            .getTableProperties
            .get(CarbonCommonConstants.COMPRESSOR)
          if (null == columnCompressor) {
            columnCompressor = CompressorFactory.getInstance.getCompressor.getName
          }
          var blockNameFromTupleID =
            if (TID.contains("#/") && externalSegmentPath.isDefined) {
              CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                TupleIdEnum.EXTERNAL_BLOCK_ID)
            } else {
              CarbonUpdateUtil.getRequiredFieldFromTID(TID,
                TupleIdEnum.BLOCK_ID)
            }
          blockNameFromTupleID = blockNameFromTupleID.replace(CarbonCommonConstants.UNDERSCORE,
            CarbonTablePath.BATCH_PREFIX)
          val completeBlockName = if (carbonTable.isHivePartitionTable) {
            CarbonTablePath
              .addDataPartPrefix(
                blockNameFromTupleID + CarbonCommonConstants.POINT + columnCompressor +
                CarbonCommonConstants.FACT_FILE_EXT)
          } else {
            CarbonTablePath
              .addDataPartPrefix(
                blockNameFromTupleID + CarbonCommonConstants.POINT + columnCompressor +
                CarbonCommonConstants.FACT_FILE_EXT)
          }
          val deleteDeltaPath = CarbonUpdateUtil
            .getDeleteDeltaFilePath(blockPath, blockName, timestamp)
          val carbonDeleteWriter = new CarbonDeleteDeltaWriterImpl(deleteDeltaPath)



          segmentUpdateDetails.setBlockName(blockName)
          segmentUpdateDetails.setActualBlockName(completeBlockName)
          segmentUpdateDetails.setSegmentName(segmentId)
          segmentUpdateDetails.setDeleteDeltaEndTimestamp(timestamp)
          segmentUpdateDetails.setDeleteDeltaStartTimestamp(timestamp)

          val alreadyDeletedRows: Long = rowCountDetailsVO.getDeletedRowsInBlock
          val totalDeletedRows: Long = alreadyDeletedRows + countOfRows
          segmentUpdateDetails.setDeletedRowsInBlock(totalDeletedRows.toString)
          if (totalDeletedRows == rowCountDetailsVO.getTotalNumberOfRows) {
            segmentUpdateDetails.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
          } else {
            // write the delta file
            carbonDeleteWriter.write(deleteDeltaBlockDetails)
          }

          deleteStatus = SegmentStatus.SUCCESS
        } catch {
          case e : MultipleMatchingException =>
            LOGGER.error(e.getMessage)
          // don't throw exception here.
          case e: Exception =>
            val errorMsg = s"Delete data operation is failed for" +
              s" ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }."
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

        override def next(): (SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long)) = {
          finished = true
          result.getKey(deleteStatus, (segmentUpdateDetails, executorErrors, countOfRows.toLong))
        }
      }
      resultIter
    }

    (res, blockMappingVO)
  }

  // all or none : update status file, only if complete delete operation is successful.
  def checkAndUpdateStatusFiles(
      executorErrors: ExecutionErrors,
      carbonTable: CarbonTable,
      timestamp: String,
      isUpdateOperation: Boolean,
      updateModel: UpdateTableModel,
      blockUpdateDetailsList: java.util.List[SegmentUpdateDetails],
      updatedSegmentList: util.Set[String],
      deletedSegmentList: util.Set[String]): Unit = {

    // Step1. write updatetablestatus file
    val updateSegmentStatus = CarbonUpdateUtil
      .updateSegmentStatus(blockUpdateDetailsList, carbonTable, timestamp, false)

    if (!updateSegmentStatus) {
      val errorMessage = "Update data operation is failed due to failure " +
        "in updatetablestatus update."
      LOGGER.error("Delete data operation is failed due to failure in updatetablestatus update.")
      executorErrors.failureCauses = FailureCauses.STATUS_FILE_UPDATION_FAILURE
      executorErrors.errorMsg = errorMessage
      throw new Exception(executorErrors.errorMsg)
    }

    // Step2. update tablestauts file
    var newMetaEntry: LoadMetadataDetails = null
    if (isUpdateOperation && updateModel.addedLoadDetail.isDefined) {
      newMetaEntry = updateModel.addedLoadDetail.get
    }
    val updateTableMetadataStatus = CarbonUpdateUtil.updateTableMetadataStatus(updatedSegmentList,
        carbonTable, timestamp, true,
        true, deletedSegmentList,
        Collections.emptySet(), "", newMetaEntry)

    if (updateTableMetadataStatus) {
      LOGGER.info(s"Delete data operation is successful for " +
        s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    } else {
      val errorMessage = "Update data operation is failed due to failure " +
        "in table status update."
      LOGGER.error("Delete data operation is failed due to failure in table status update.")
      executorErrors.failureCauses = FailureCauses.STATUS_FILE_UPDATION_FAILURE
      executorErrors.errorMsg = errorMessage
      throw new Exception(executorErrors.errorMsg)
    }
  }

  // all or none : update status file, only if complete delete operation is successful.
  def processSegments(executorErrors: ExecutionErrors,
      res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]],
      blockMappingVO: BlockMappingVO)
  : (util.List[SegmentUpdateDetails], util.HashSet[String], util.HashSet[String]) = {
    val blockUpdateDetailsList = new util.ArrayList[SegmentUpdateDetails]()
    val updatedSegmentDetails = new util.HashSet[String]()
    res.foreach(resultOfSeg => resultOfSeg.foreach(
      resultOfBlock => {
        if (resultOfBlock._1 == SegmentStatus.SUCCESS) {
          blockUpdateDetailsList.add(resultOfBlock._2._1)
          updatedSegmentDetails.add(resultOfBlock._2._1.getSegmentName)
          // if this block is invalid then decrement block count in map.
          if (CarbonUpdateUtil.isBlockInvalid(resultOfBlock._2._1.getSegmentStatus)) {
            CarbonUpdateUtil.decrementDeletedBlockCount(resultOfBlock._2._1,
              blockMappingVO.getSegmentNumberOfBlockMapping)
          }
        } else {
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
          return (blockUpdateDetailsList, new util.HashSet[String](), new util.HashSet[String]())
        }
      }))

    val deletedSegmentDetails = CarbonUpdateUtil
      .getListOfSegmentsToMarkDeleted(blockMappingVO.getSegmentNumberOfBlockMapping)

    (blockUpdateDetailsList, updatedSegmentDetails,
      new util.HashSet[String](deletedSegmentDetails))
  }

  /**
   * This function fires an event for pre-priming in the index server
   */
  def reloadDistributedSegmentCache(carbonTable: CarbonTable, deletedSegments: Seq[Segment],
      operationContext: OperationContext)(sparkSession: SparkSession): Unit = {
    if (carbonTable.isTransactionalTable) {
      val indexServerEnabled = CarbonProperties.getInstance().isDistributedPruningEnabled(
        carbonTable.getDatabaseName, carbonTable.getTableName)
      val prePrimingEnabled = CarbonProperties.getInstance().isIndexServerPrePrimingEnabled()
      if (indexServerEnabled && prePrimingEnabled) {
        val readCommittedScope = new TableStatusReadCommittedScope(AbsoluteTableIdentifier.from(
          carbonTable.getTablePath), FileFactory.getConfiguration)
        deletedSegments.foreach(_.setReadCommittedScope(readCommittedScope))
        LOGGER.info(s"Loading segments for table: ${ carbonTable.getTableName } in the cache")
        val indexServerLoadEvent: IndexServerLoadEvent =
          IndexServerLoadEvent(
            sparkSession,
            carbonTable,
            deletedSegments.toList,
            deletedSegments.map(_.getSegmentNo).toList
          )
        OperationListenerBus.getInstance().fireEvent(indexServerLoadEvent, operationContext)
        LOGGER.info(s"Segments for table: ${
          carbonTable
            .getTableName
        } has been loaded in the cache")
      } else {
        LOGGER.info(
          s"Segments for table:" + s" ${ carbonTable.getTableName } not loaded in the cache")
      }
    }
  }

  private def createCarbonInputFormat(absoluteTableIdentifier: AbsoluteTableIdentifier) :
  (CarbonTableInputFormat[Array[Object]], Job) = {
    val carbonInputFormat = new CarbonTableInputFormat[Array[Object]]()
    val job: Job = CarbonSparkUtil.createHadoopJob()
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    (carbonInputFormat, job)
  }
}
