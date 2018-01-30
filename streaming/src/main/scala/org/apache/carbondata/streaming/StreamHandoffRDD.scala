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

package org.apache.carbondata.streaming

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.streaming.{CarbonStreamInputFormat, CarbonStreamRecordReader}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CompactionResultSortProcessor, CompactionType}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.{HandoffResult, HandoffResultImpl}
import org.apache.carbondata.spark.rdd.CarbonRDD

/**
 * partition of the handoff segment
 */
class HandoffPartition(
    val rddId: Int,
    val idx: Int,
    @transient val inputSplit: CarbonInputSplit
) extends Partition {

  val split = new SerializableWritable[CarbonInputSplit](inputSplit)

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * package the record reader of the handoff segment to RawResultIterator
 */
class StreamingRawResultIterator(
    recordReader: CarbonStreamRecordReader
) extends RawResultIterator(null, null, null) {

  override def hasNext: Boolean = {
    recordReader.nextKeyValue()
  }

  override def next(): Array[Object] = {
    val rowTmp = recordReader
      .getCurrentValue
      .asInstanceOf[GenericInternalRow]
      .values
      .asInstanceOf[Array[Object]]
    val row = new Array[Object](rowTmp.length)
    System.arraycopy(rowTmp, 0, row, 0, rowTmp.length)
    row
  }
}

/**
 * execute streaming segment handoff
 */
class StreamHandoffRDD[K, V](
    sc: SparkContext,
    result: HandoffResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    handOffSegmentId: String
) extends CarbonRDD[(K, V)](sc, Nil) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def internalCompute(
      split: Partition,
      context: TaskContext
  ): Iterator[(K, V)] = {
    carbonLoadModel.setTaskNo("" + split.index)
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    CarbonMetadata.getInstance().addCarbonTable(carbonTable)
    // the input iterator is using raw row
    val iteratorList = prepareInputIterator(split, carbonTable)
    // use CompactionResultSortProcessor to sort data dan write to columnar files
    val processor = prepareHandoffProcessor(carbonTable)
    val status = processor.execute(iteratorList)

    new Iterator[(K, V)] {
      private var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey("" + split.index, status)
      }
    }
  }

  /**
   * prepare input iterator by basing CarbonStreamRecordReader
   */
  private def prepareInputIterator(
      split: Partition,
      carbonTable: CarbonTable
  ): util.ArrayList[RawResultIterator] = {
    val inputSplit = split.asInstanceOf[HandoffPartition].split.value
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val hadoopConf = new Configuration()
    CarbonTableInputFormat.setDatabaseName(hadoopConf, carbonTable.getDatabaseName)
    CarbonTableInputFormat.setTableName(hadoopConf, carbonTable.getTableName)
    CarbonTableInputFormat.setTablePath(hadoopConf, carbonTable.getTablePath)
    val projection = new CarbonProjection
    val dataFields = carbonTable.getStreamStorageOrderColumn(carbonTable.getTableName)
    (0 until dataFields.size()).foreach { index =>
      projection.addColumn(dataFields.get(index).getColName)
    }
    CarbonTableInputFormat.setColumnProjection(hadoopConf, projection)
    CarbonTableInputFormat.setTableInfo(hadoopConf, carbonTable.getTableInfo)
    val attemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId)
    val format = new CarbonTableInputFormat[Array[Object]]()
    val model = format.createQueryModel(inputSplit, attemptContext)
    val inputFormat = new CarbonStreamInputFormat
    val streamReader = inputFormat.createRecordReader(inputSplit, attemptContext)
      .asInstanceOf[CarbonStreamRecordReader]
    streamReader.setVectorReader(false)
    streamReader.setQueryModel(model)
    streamReader.setUseRawRow(true)
    streamReader.initialize(inputSplit, attemptContext)
    val iteratorList = new util.ArrayList[RawResultIterator](1)
    iteratorList.add(new StreamingRawResultIterator(streamReader))
    iteratorList
  }

  private def prepareHandoffProcessor(
      carbonTable: CarbonTable
  ): CompactionResultSortProcessor = {
    val wrapperColumnSchemaList = CarbonUtil.getColumnSchemaList(
      carbonTable.getDimensionByTableName(carbonTable.getTableName),
      carbonTable.getMeasureByTableName(carbonTable.getTableName))
    val dimLensWithComplex =
      (0 until wrapperColumnSchemaList.size()).map(_ => Integer.MAX_VALUE).toArray
    val dictionaryColumnCardinality =
      CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList)
    val segmentProperties =
      new SegmentProperties(wrapperColumnSchemaList, dictionaryColumnCardinality)

    new CompactionResultSortProcessor(
      carbonLoadModel,
      carbonTable,
      segmentProperties,
      CompactionType.STREAMING,
      carbonTable.getTableName,
      null
    )
  }

  /**
   * get the partitions of the handoff segment
   */
  override protected def getPartitions: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val inputFormat = new CarbonTableInputFormat[Array[Object]]()
    val segmentList = new util.ArrayList[String](1)
    segmentList.add(handOffSegmentId)
    val splits = inputFormat.getSplitsOfStreaming(
      job,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier,
      segmentList
    )

    (0 until splits.size()).map { index =>
      new HandoffPartition(id, index, splits.get(index).asInstanceOf[CarbonInputSplit])
    }.toArray[Partition]
  }
}

object StreamHandoffRDD {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def iterateStreamingHandoff(
      carbonLoadModel: CarbonLoadModel,
      sparkSession: SparkSession
  ): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val identifier = carbonTable.getAbsoluteTableIdentifier
    val tablePath = CarbonStorePath.getCarbonTablePath(identifier)
    var continueHandoff = false
    // require handoff lock on table
    val lock = CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.HANDOFF_LOCK)
    try {
      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the handoff lock for table" +
                    s" ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
        // handoff streaming segment one by one
        do {
          val segmentStatusManager = new SegmentStatusManager(identifier)
          var loadMetadataDetails: Array[LoadMetadataDetails] = null
          // lock table to read table status file
          val statusLock = segmentStatusManager.getTableStatusLock
          try {
            if (statusLock.lockWithRetries()) {
              loadMetadataDetails = SegmentStatusManager.readLoadMetadata(
                tablePath.getMetadataDirectoryPath)
            }
          } finally {
            if (null != statusLock) {
              statusLock.unlock()
            }
          }
          if (null != loadMetadataDetails) {
            val streamSegments =
              loadMetadataDetails.filter(_.getSegmentStatus == SegmentStatus.STREAMING_FINISH)

            continueHandoff = streamSegments.length > 0
            if (continueHandoff) {
              // handoff a streaming segment
              val loadMetadataDetail = streamSegments(0)
              executeStreamingHandoff(
                carbonLoadModel,
                sparkSession,
                loadMetadataDetail.getLoadName
              )
            }
          } else {
            continueHandoff = false
          }
        } while (continueHandoff)
      }
    } finally {
      if (null != lock) {
        lock.unlock()
      }
    }
  }

  /**
   * start new thread to execute stream segment handoff
   */
  def startStreamingHandoffThread(
      carbonLoadModel: CarbonLoadModel,
      sparkSession: SparkSession
  ): Unit = {
    // start a new thread to execute streaming segment handoff
    val handoffThread = new Thread() {
      override def run(): Unit = {
        iterateStreamingHandoff(carbonLoadModel, sparkSession)
      }
    }
    handoffThread.start()
  }

  /**
   * invoke StreamHandoffRDD to handoff a streaming segment to a columnar segment
   */
  def executeStreamingHandoff(
      carbonLoadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      handoffSegmenId: String
  ): Unit = {
    var loadStatus = SegmentStatus.SUCCESS
    var errorMessage: String = "Handoff failure"
    try {
      // generate new columnar segment
      val newMetaEntry = new LoadMetadataDetails
      carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      CarbonLoaderUtil.populateNewLoadMetaEntry(
        newMetaEntry,
        SegmentStatus.INSERT_IN_PROGRESS,
        carbonLoadModel.getFactTimeStamp,
        false)
      CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, carbonLoadModel, true, false)
      // convert a streaming segment to columnar segment
      val status = new StreamHandoffRDD(
        sparkSession.sparkContext,
        new HandoffResultImpl(),
        carbonLoadModel,
        handoffSegmenId).collect()

      status.foreach { x =>
        if (!x._2) {
          loadStatus = SegmentStatus.LOAD_FAILURE
        }
      }
    } catch {
      case ex: Exception =>
        loadStatus = SegmentStatus.LOAD_FAILURE
        errorMessage = errorMessage + ": " + ex.getCause.getMessage
        LOGGER.error(errorMessage)
        LOGGER.error(ex, s"Handoff failed on streaming segment $handoffSegmenId")
    }

    if (loadStatus == SegmentStatus.LOAD_FAILURE) {
      CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel)
      LOGGER.info("********starting clean up**********")
      CarbonLoaderUtil.deleteSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)
      LOGGER.info("********clean up done**********")
      LOGGER.audit(s"Handoff is failed for " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      LOGGER.warn("Cannot write load metadata file as handoff failed")
      throw new Exception(errorMessage)
    }

    if (loadStatus == SegmentStatus.SUCCESS) {
      val done = updateLoadMetadata(handoffSegmenId, carbonLoadModel)
      if (!done) {
        val errorMessage = "Handoff failed due to failure in table status updation."
        LOGGER.audit("Handoff is failed for " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.error("Handoff failed due to failure in table status updation.")
        throw new Exception(errorMessage)
      }
      done
    }

  }

  /**
   * update streaming segment and new columnar segment
   */
  private def updateLoadMetadata(
      handoffSegmentId: String,
      loadModel: CarbonLoadModel
  ): Boolean = {
    var status = false
    val metaDataFilepath =
      loadModel.getCarbonDataLoadSchema().getCarbonTable().getMetaDataFilepath()
    val identifier =
      loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier()
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(identifier)
    val metadataPath = carbonTablePath.getMetadataDirectoryPath()
    val fileType = FileFactory.getFileType(metadataPath)
    if (!FileFactory.isFileExist(metadataPath, fileType)) {
      FileFactory.mkdirs(metadataPath, fileType)
    }
    val tableStatusPath = carbonTablePath.getTableStatusFilePath()
    val segmentStatusManager = new SegmentStatusManager(identifier)
    val carbonLock = segmentStatusManager.getTableStatusLock()
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
          "Acquired lock for table" + loadModel.getDatabaseName() + "." + loadModel.getTableName()
          + " for table status updation")
        val listOfLoadFolderDetailsArray =
          SegmentStatusManager.readLoadMetadata(metaDataFilepath)

        // update new columnar segment to success status
        val newSegment =
          listOfLoadFolderDetailsArray.find(_.getLoadName.equals(loadModel.getSegmentId))
        if (newSegment.isEmpty) {
          throw new Exception("Failed to update table status for new segment")
        } else {
          newSegment.get.setSegmentStatus(SegmentStatus.SUCCESS)
          newSegment.get.setLoadEndTime(System.currentTimeMillis())
        }

        // update streaming segment to compacted status
        val streamSegment =
          listOfLoadFolderDetailsArray.find(_.getLoadName.equals(handoffSegmentId))
        if (streamSegment.isEmpty) {
          throw new Exception("Failed to update table status for streaming segment")
        } else {
          streamSegment.get.setSegmentStatus(SegmentStatus.COMPACTED)
        }

        // refresh table status file
        SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray)
        status = true
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + loadModel
          .getDatabaseName() + "." + loadModel.getTableName());
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" +
                    loadModel.getDatabaseName() + "." + loadModel.getTableName())
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + loadModel.getDatabaseName() +
                     "." + loadModel.getTableName() + " during table status updation")
      }
    }
    return status
  }
}
