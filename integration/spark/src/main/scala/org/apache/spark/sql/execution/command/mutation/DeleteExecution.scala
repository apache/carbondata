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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
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
import org.apache.carbondata.spark.DeleteDelataResultImpl

object DeleteExecution {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def deleteDeltaExecution(
      databaseNameOp: Option[String],
      tableName: String,
      sparkSession: SparkSession,
      dataRdd: RDD[Row],
      timestamp: String,
      isUpdateOperation: Boolean,
      executorErrors: ExecutionErrors): (Seq[Segment], Long) = {

    val (res, blockMappingVO) = deleteDeltaExecutionInternal(databaseNameOp,
      tableName, sparkSession, dataRdd, timestamp, isUpdateOperation, executorErrors)
    var segmentsTobeDeleted = Seq.empty[Segment]
    var operatedRowCount = 0L
    // if no loads are present then no need to do anything.
    if (res.flatten.isEmpty) {
      return (segmentsTobeDeleted, operatedRowCount)
    }
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    // update new status file
    segmentsTobeDeleted =
      checkAndUpdateStatusFiles(executorErrors,
        res, carbonTable, timestamp,
        blockMappingVO, isUpdateOperation)

    if (executorErrors.failureCauses == FailureCauses.NONE) {
      operatedRowCount = res.flatten.map(_._2._3).sum
    }
    (segmentsTobeDeleted, operatedRowCount)
  }

  /**
   * generate the delete delta files in each segment as per the RDD.
   * @return it gives the segments which needs to be deleted.
   */
  def deleteDeltaExecutionInternal(
      databaseNameOp: Option[String],
      tableName: String,
      sparkSession: SparkSession,
      dataRdd: RDD[Row],
      timestamp: String,
      isUpdateOperation: Boolean,
      executorErrors: ExecutionErrors,
      tupleId: Option[Int] = None):
  (Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]], BlockMappingVO) = {

    var res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]] = null
    val database = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
    val tablePath = absoluteTableIdentifier.getTablePath

    val deleteRdd = if (isUpdateOperation) {
      val schema =
        org.apache.spark.sql.types.StructType(Seq(org.apache.spark.sql.types.StructField(
          CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
          org.apache.spark.sql.types.StringType)))
      val rdd = tupleId match {
        case Some(id) =>
          dataRdd
            .map(row => Row(row.get(id)))
        case _ =>
          dataRdd
            .map(row => Row(row.get(row.fieldIndex(
              CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))))
      }
      sparkSession.createDataFrame(rdd, schema).rdd
    } else {
      dataRdd
    }

    val (carbonInputFormat, job) = createCarbonInputFormat(absoluteTableIdentifier)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    val keyRdd = tupleId match {
      case Some(id) =>
        deleteRdd.map { row =>
          val tupleId: String = row.getString(id)
          val key = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleId)
          (key, row)
        }.groupByKey()
      case _ =>
        deleteRdd.map { row =>
          val tupleId: String = row
            .getString(row.fieldIndex(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
          val key = CarbonUpdateUtil.getSegmentWithBlockFromTID(tupleId)
          (key, row)
        }.groupByKey()
    }

    // if no loads are present then no need to do anything.
    if (keyRdd.partitions.length == 0) {
      return (Array.empty[List[(SegmentStatus,
        (SegmentUpdateDetails, ExecutionErrors, Long))]], null)
    }
    val blockMappingVO =
      carbonInputFormat.getBlockRowCount(
        job,
        carbonTable,
        CarbonFilters.getPartitions(
          Seq.empty,
          sparkSession,
          TableIdentifier(tableName, databaseNameOp)).map(_.asJava).orNull, true)
    val segmentUpdateStatusMngr = new SegmentUpdateStatusManager(carbonTable)
    CarbonUpdateUtil
      .createBlockDetailsMap(blockMappingVO, segmentUpdateStatusMngr)

    val metadataDetails = SegmentStatusManager.readTableStatusFile(
      CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
    val isStandardTable = CarbonUtil.isStandardCarbonTable(carbonTable)
    val rowContRdd =
      sparkSession.sparkContext.parallelize(
        blockMappingVO.getCompleteBlockRowDetailVO.asScala.toSeq,
        keyRdd.partitions.length)

    val conf = SparkSQLUtil
      .broadCastHadoopConf(sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())
    val rdd = rowContRdd.join(keyRdd)
    res = rdd.mapPartitionsWithIndex(
      (index: Int, records: Iterator[((String), (RowCountDetailsVO, Iterable[Row]))]) =>
        Iterator[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]] {
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
          var result = List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]()
          while (records.hasNext) {
            val ((key), (rowCountDetailsVO, groupedRows)) = records.next
            result = result ++
                     deleteDeltaFunc(index,
                       key,
                       groupedRows.toIterator,
                       timestamp,
                       rowCountDetailsVO,
                       isStandardTable,
                       metadataDetails)
          }
          result
        }).collect()

    def deleteDeltaFunc(index: Int,
        key: String,
        iter: Iterator[Row],
        timestamp: String,
        rowCountDetailsVO: RowCountDetailsVO,
        isStandardTable: Boolean,
        loads: Array[LoadMetadataDetails]
    ): Iterator[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))] = {

      val result = new DeleteDelataResultImpl()
      var deleteStatus = SegmentStatus.LOAD_FAILURE
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
      // here key = segment/blockName
      val blockName = CarbonUpdateUtil
        .getBlockName(
          CarbonTablePath.addDataPartPrefix(key.split(CarbonCommonConstants.FILE_SEPARATOR)(1)))
      val segmentId = key.split(CarbonCommonConstants.FILE_SEPARATOR)(0)
      val load = loads.find(l => l.getLoadName.equalsIgnoreCase(segmentId)).get
      val deleteDeltaBlockDetails: DeleteDeltaBlockDetails = new DeleteDeltaBlockDetails(blockName)
      val resultIter =
        new Iterator[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))] {
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
            val pageId = Integer.parseInt(CarbonUpdateUtil
              .getRequiredFieldFromTID(TID, TupleIdEnum.PAGE_ID))
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
            if (StringUtils.isNotEmpty(load.getPath)) {
              load.getPath
            } else {
              CarbonUpdateUtil.getTableBlockPath(TID, tablePath, isStandardTable)
            }
          val completeBlockName = CarbonTablePath
            .addDataPartPrefix(CarbonUpdateUtil.getRequiredFieldFromTID(TID, TupleIdEnum.BLOCK_ID) +
                               CarbonCommonConstants.FACT_FILE_EXT)
          val deleteDeletaPath = CarbonUpdateUtil
            .getDeleteDeltaFilePath(blockPath, blockName, timestamp)
          val carbonDeleteWriter = new CarbonDeleteDeltaWriterImpl(deleteDeletaPath)



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
          }
          else {
            // write the delta file
            carbonDeleteWriter.write(deleteDeltaBlockDetails)
          }

          deleteStatus = SegmentStatus.SUCCESS
        } catch {
          case e : MultipleMatchingException =>
            LOGGER.error(e.getMessage)
          // don't throw exception here.
          case e: Exception =>
            val errorMsg = s"Delete data operation is failed for ${ database }.${ tableName }."
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

  // all or none : update status file, only if complete delete opeartion is successfull.
  def checkAndUpdateStatusFiles(
      executorErrors: ExecutionErrors,
      res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]],
      carbonTable: CarbonTable,
      timestamp: String,
      blockMappingVO: BlockMappingVO,
      isUpdateOperation: Boolean): Seq[Segment] = {
    val blockUpdateDetailsList = new util.ArrayList[SegmentUpdateDetails]()
    val segmentDetails = new util.HashSet[Segment]()
    res.foreach(resultOfSeg => resultOfSeg.foreach(
      resultOfBlock => {
        if (resultOfBlock._1 == SegmentStatus.SUCCESS) {
          blockUpdateDetailsList.add(resultOfBlock._2._1)
          segmentDetails.add(new Segment(resultOfBlock._2._1.getSegmentName))
          // if this block is invalid then decrement block count in map.
          if (CarbonUpdateUtil.isBlockInvalid(resultOfBlock._2._1.getSegmentStatus)) {
            CarbonUpdateUtil.decrementDeletedBlockCount(resultOfBlock._2._1,
              blockMappingVO.getSegmentNumberOfBlockMapping)
          }
        } else {
          // In case of failure , clean all related delete delta files
          CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
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
          return Seq.empty[Segment]
        }
      }))

    val listOfSegmentToBeMarkedDeleted = CarbonUpdateUtil
      .getListOfSegmentsToMarkDeleted(blockMappingVO.getSegmentNumberOfBlockMapping)

    val segmentsTobeDeleted = listOfSegmentToBeMarkedDeleted.asScala

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
      LOGGER.info(s"Delete data operation is successful for " +
                  s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    } else {
      // In case of failure , clean all related delete delta files
      CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
      val errorMessage = "Delete data operation is failed due to failure " +
                         "in table status updation."
      LOGGER.error("Delete data operation is failed due to failure in table status updation.")
      executorErrors.failureCauses = FailureCauses.STATUS_FILE_UPDATION_FAILURE
      executorErrors.errorMsg = errorMessage
    }
    segmentsTobeDeleted
  }

  // all or none : update status file, only if complete delete opeartion is successfull.
  def processSegments(executorErrors: ExecutionErrors,
      res: Array[List[(SegmentStatus, (SegmentUpdateDetails, ExecutionErrors, Long))]],
      carbonTable: CarbonTable,
      timestamp: String,
      blockMappingVO: BlockMappingVO): (util.List[SegmentUpdateDetails], Seq[Segment]) = {
    val blockUpdateDetailsList = new util.ArrayList[SegmentUpdateDetails]()
    val segmentDetails = new util.HashSet[Segment]()
    res.foreach(resultOfSeg => resultOfSeg.foreach(
      resultOfBlock => {
        if (resultOfBlock._1 == SegmentStatus.SUCCESS) {
          blockUpdateDetailsList.add(resultOfBlock._2._1)
          segmentDetails.add(new Segment(resultOfBlock._2._1.getSegmentName))
          // if this block is invalid then decrement block count in map.
          if (CarbonUpdateUtil.isBlockInvalid(resultOfBlock._2._1.getSegmentStatus)) {
            CarbonUpdateUtil.decrementDeletedBlockCount(resultOfBlock._2._1,
              blockMappingVO.getSegmentNumberOfBlockMapping)
          }
        } else {
          // In case of failure , clean all related delete delta files
          CarbonUpdateUtil.cleanStaleDeltaFiles(carbonTable, timestamp)
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
          return (blockUpdateDetailsList, Seq.empty[Segment])
        }
      }))

    val listOfSegmentToBeMarkedDeleted = CarbonUpdateUtil
      .getListOfSegmentsToMarkDeleted(blockMappingVO.getSegmentNumberOfBlockMapping)

    (blockUpdateDetailsList, listOfSegmentToBeMarkedDeleted.asScala)
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
    val jobConf: JobConf = new JobConf(FileFactory.getConfiguration)
    val job: Job = new Job(jobConf)
    FileInputFormat.addInputPath(job, new Path(absoluteTableIdentifier.getTablePath))
    (carbonInputFormat, job)
  }
}
