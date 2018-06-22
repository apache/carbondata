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

package org.apache.carbondata.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.statusmanager._
import org.apache.carbondata.core.util.{CarbonUtil, DataTypeUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostStatusUpdateEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CompactionResultSortProcessor, CompactionType}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.{HandoffResult, HandoffResultImpl}
import org.apache.carbondata.spark.util.{CommonUtil, SparkDataTypeConverterImpl}
import org.apache.carbondata.streaming.{CarbonStreamInputFormat, CarbonStreamRecordReader}


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
 * todo: actually we should not extends rawResultIterator if we don't use any method or variable
 * from it. We only use it to reduce duplicate code for compaction and handoff
 * and we can extract it later
 */
class StreamingRawResultIterator(
    recordReader: CarbonStreamRecordReader
) extends RawResultIterator(null, null, null, true) {

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
) extends CarbonRDD[(K, V)](sc, Nil, sc.hadoopConfiguration) {

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
    DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
    CarbonMetadata.getInstance().addCarbonTable(carbonTable)
    // the input iterator is using raw row
    val iteratorList = prepareInputIterator(split, carbonTable)

    CommonUtil.setTempStoreLocation(split.index, carbonLoadModel, true, false)
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
    CarbonInputFormat.setDatabaseName(hadoopConf, carbonTable.getDatabaseName)
    CarbonInputFormat.setTableName(hadoopConf, carbonTable.getTableName)
    CarbonInputFormat.setTablePath(hadoopConf, carbonTable.getTablePath)
    val projection = new CarbonProjection
    val dataFields = carbonTable.getStreamStorageOrderColumn(carbonTable.getTableName)
    (0 until dataFields.size()).foreach { index =>
      projection.addColumn(dataFields.get(index).getColName)
    }
    CarbonInputFormat.setColumnProjection(hadoopConf, projection)
    CarbonInputFormat.setTableInfo(hadoopConf, carbonTable.getTableInfo)
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
    val segmentList = new util.ArrayList[Segment](1)
    segmentList.add(Segment.toSegment(handOffSegmentId, null))
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

  private val manager = new SegmentManager()

  def iterateStreamingHandoff(
      carbonLoadModel: CarbonLoadModel,
      operationContext: OperationContext,
      sparkSession: SparkSession): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val identifier = carbonTable.getAbsoluteTableIdentifier
    var continueHandoff = false
    // require handoff lock on table
    val lock = CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.HANDOFF_LOCK)
    try {
      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the handoff lock for table" +
                    s" ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
        // handoff streaming segment one by one
        do {
          val detailVOs =
            manager.getAllSegments(identifier).getAllSegments.asScala
          if (null != detailVOs) {
            val streamSegments =
              detailVOs.filter(_.getStatus().equals(SegmentStatus.STREAMING_FINISH.toString))

            continueHandoff = streamSegments.nonEmpty
            if (continueHandoff) {
              // handoff a streaming segment
              val loadMetadataDetail = streamSegments.head
              carbonLoadModel.setSegmentId(loadMetadataDetail.getSegmentId)
              executeStreamingHandoff(
                carbonLoadModel,
                sparkSession,
                operationContext,
                loadMetadataDetail.getSegmentId
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
      operationContext: OperationContext,
      sparkSession: SparkSession,
      isDDL: Boolean
  ): Unit = {
    if (isDDL) {
      iterateStreamingHandoff(carbonLoadModel, operationContext, sparkSession)
    } else {
      // start a new thread to execute streaming segment handoff
      val handoffThread = new Thread() {
        override def run(): Unit = {
          iterateStreamingHandoff(carbonLoadModel, operationContext, sparkSession)
        }
      }
      handoffThread.start()
    }
  }

  /**
   * invoke StreamHandoffRDD to handoff a streaming segment to a columnar segment
   */
  def executeStreamingHandoff(
      carbonLoadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      operationContext: OperationContext,
      handoffSegmenId: String): Unit = {
    var loadStatus = SegmentStatus.SUCCESS
    var errorMessage: String = "Handoff failure"
    try {
      // generate new columnar segment
      val detailVO = manager.createNewSegment(
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier,
        new SegmentDetailVO)

      carbonLoadModel.setCurrentDetailVO(detailVO)
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
      val detailVO =
        SegmentManagerHelper.updateFailStatusAndGetSegmentVO(carbonLoadModel.getSegmentId)
      manager
        .commitLoadSegment(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
          .getAbsoluteTableIdentifier, detailVO)
      LOGGER.info("********starting clean up**********")
      CarbonLoaderUtil.deleteSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)
      LOGGER.info("********clean up done**********")
      LOGGER.audit(s"Handoff is failed for " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      LOGGER.error("Cannot write load metadata file as handoff failed")
      throw new Exception(errorMessage)
    }

    if (loadStatus == SegmentStatus.SUCCESS) {
      operationContext.setProperty("uuid", UUID.randomUUID().toString)
      val loadTablePreStatusUpdateEvent: LoadTablePreStatusUpdateEvent =
        new LoadTablePreStatusUpdateEvent(
          carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getCarbonTableIdentifier,
          carbonLoadModel)
      OperationListenerBus.getInstance().fireEvent(loadTablePreStatusUpdateEvent, operationContext)

      val done = updateLoadMetadata(handoffSegmenId, carbonLoadModel)

      val loadTablePostStatusUpdateEvent: LoadTablePostStatusUpdateEvent =
        new LoadTablePostStatusUpdateEvent(carbonLoadModel)
      OperationListenerBus.getInstance()
        .fireEvent(loadTablePostStatusUpdateEvent, operationContext)
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
    val metaDataFilepath = loadModel.getCarbonDataLoadSchema.getCarbonTable.getMetadataPath
    val identifier = loadModel.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier
    val metadataPath = CarbonTablePath.getMetadataPath(identifier.getTablePath)
    val fileType = FileFactory.getFileType(metadataPath)
    if (!FileFactory.isFileExist(metadataPath, fileType)) {
      FileFactory.mkdirs(metadataPath, fileType)
    }
    val updateList = new util.ArrayList[SegmentDetailVO]()
    updateList.add(new SegmentDetailVO().setSegmentId(loadModel.getSegmentId)
        .setStatus(SegmentStatus.SUCCESS.toString).setLoadEndTime(System.currentTimeMillis()))
    updateList.add(new SegmentDetailVO().setSegmentId(handoffSegmentId)
        .setStatus(SegmentStatus.COMPACTED.toString).setMergedSegmentIds(loadModel.getSegmentId))
    manager.updateSegments(identifier, updateList)
  }
}
