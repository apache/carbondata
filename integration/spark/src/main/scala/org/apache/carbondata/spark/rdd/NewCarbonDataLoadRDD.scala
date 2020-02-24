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

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionWrap, RDD}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, DataTypeUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.{DataLoadExecutor, FailureCauses, TableProcessingOperations}
import org.apache.carbondata.processing.loading.csvinput.{BlockDetails, CSVInputFormat, CSVRecordReaderIterator}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonQueryUtil
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil, Util}

/**
 * This partition class use to split by Host
 *
 */
class CarbonNodePartition(rddId: Int, val idx: Int, host: String,
    val blocksDetails: Array[BlockDetails])
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = host
  val nodeBlocksDetail = blocksDetails

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

class SparkPartitionLoader(model: CarbonLoadModel,
    splitIndex: Long,
    storePath: String,
    loadMetadataDetails: LoadMetadataDetails) {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  var storeLocation: Array[String] = Array[String]()

  def initialize(): Unit = {
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties")
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance.initPartitionInfo(
      CarbonTablePath.DEPRECATED_PARTITION_ID)
    CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true")
    CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1")
    CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true")
    CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true")
    CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true")
    CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false")

    storeLocation = CommonUtil.getTempStoreLocations(splitIndex.toString)
    LOGGER.info("Temp location for loading data: " + storeLocation.mkString(","))
  }
}

/**
 * It loads the data to carbon using @AbstractDataLoadProcessorStep
 */
class NewCarbonDataLoadRDD[K, V](
    @transient private val ss: SparkSession,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    blocksGroupBy: Array[(String, Array[BlockDetails])])
  extends CarbonRDD[(K, V)](ss, Nil) {

  ss.sparkContext.setLocalProperty("spark.scheduler.pool", "DDL")

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def internalGetPartitions: Array[Partition] = {
    blocksGroupBy.zipWithIndex.map { b =>
      new CarbonNodePartition(id, b._2, b._1._1, b._1._2)
    }
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      val loadMetadataDetails = new LoadMetadataDetails()
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      var model: CarbonLoadModel = _
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {
        loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS)

        val preFetch = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
          .USE_PREFETCH_WHILE_LOADING, CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING_DEFAULT)
        carbonLoadModel.setPreFetch(preFetch.toBoolean)
        val recordReaders = getInputIterators
        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null,
          loadMetadataDetails)
        // Initialize to set carbon properties
        loader.initialize()
        // need to clear thread local before every load.
        DataTypeUtil.clearFormatter()
        val executor = new DataLoadExecutor()
        // in case of success, failure or cancelation clear memory and stop execution
        context
          .addTaskCompletionListener { new InsertTaskCompletionListener(executor, executionErrors) }
        executor.execute(model,
          loader.storeLocation,
          recordReaders)
      } catch {
        case e: NoRetryException =>
          loadMetadataDetails.setSegmentStatus(SegmentStatus.LOAD_PARTIAL_SUCCESS)
          executionErrors.failureCauses = FailureCauses.BAD_RECORDS
          executionErrors.errorMsg = e.getMessage
          logInfo("Bad Record Found")
        case e: Exception =>
          loadMetadataDetails.setSegmentStatus(SegmentStatus.LOAD_FAILURE)
          executionErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
          executionErrors.errorMsg = e.getMessage
          logInfo("DataLoad failure", e)
          LOGGER.error(e)
          throw e
      } finally {
        // clean up the folders and files created locally for data load operation
        TableProcessingOperations.deleteLocalDataLoadFolderLocation(model, false, false)
        // in case of failure the same operation will be re-tried several times.
        // So print the data load statistics only in case of non failure case
        if (SegmentStatus.LOAD_FAILURE != loadMetadataDetails.getSegmentStatus) {
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance
            .printStatisticsInfo(CarbonTablePath.DEPRECATED_PARTITION_ID)
        }
      }

      def getInputIterators: Array[CarbonIterator[Array[AnyRef]]] = {
        val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, theSplit.index, 0)
        val configuration: Configuration = FileFactory.getConfiguration
        CommonUtil.configureCSVInputFormat(configuration, carbonLoadModel)
        val hadoopAttemptContext = new TaskAttemptContextImpl(configuration, attemptId)
        val format = new CSVInputFormat

        val split = theSplit.asInstanceOf[CarbonNodePartition]
        val inputSize = split.blocksDetails.map(_.getBlockLength).sum * 0.1 * 10  / 1024 / 1024
        logInfo("Input split: " + split.serializableHadoopSplit)
        logInfo("The block count in this node: " + split.nodeBlocksDetail.length)
        logInfo(f"The input data size in this node: $inputSize%.2fMB")
        CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordHostBlockMap(
            split.serializableHadoopSplit, split.nodeBlocksDetail.length)
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        val fileList: java.util.List[String] = new java.util.ArrayList[String](
            CarbonCommonConstants.CONSTANT_SIZE_TEN)
        CarbonQueryUtil.splitFilePath(carbonLoadModel.getFactFilePath, fileList, ",")
        model = carbonLoadModel.getCopyWithPartition(
          carbonLoadModel.getCsvHeader, carbonLoadModel.getCsvDelimiter)
        val readers =
          split.nodeBlocksDetail.map(format.createRecordReader(_, hadoopAttemptContext))
        readers.zipWithIndex.map { case (reader, index) =>
          new CSVRecordReaderIterator(reader, split.nodeBlocksDetail(index), hadoopAttemptContext)
        }
      }
      /**
       * generate blocks id
       *
       * @return
       */
      def generateBlocksID: String = {
        carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName + "_" +
        UUID.randomUUID()
      }

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, (loadMetadataDetails, executionErrors))
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonNodePartition]
    val firstOptionLocation: Seq[String] = List(theSplit.serializableHadoopSplit)
    logInfo("Preferred Location for split : " + firstOptionLocation.mkString(","))
    /**
     * At original logic, we were adding the next preferred location so that in case of the
     * failure the Spark should know where to schedule the failed task.
     * Remove the next preferred location is because some time Spark will pick the same node
     * for 2 tasks, so one node is getting over loaded with the task and one have no task to
     * do. And impacting the performance despite of any failure.
     */
    firstOptionLocation
  }
}

/**
 *  It loads the data to carbon from spark DataFrame using
 *  @see org.apache.carbondata.processing.newflow.DataLoadExecutor
 */
class NewDataFrameLoaderRDD[K, V](
    @transient private val ss: SparkSession,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    prev: DataLoadCoalescedRDD[_]) extends CarbonRDD[(K, V)](ss, prev) {

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      val loadMetadataDetails = new LoadMetadataDetails()
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      val model: CarbonLoadModel = carbonLoadModel
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {
        loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS)
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        carbonLoadModel.setPreFetch(false)
        val recordReaders = mutable.Buffer[CarbonIterator[Array[AnyRef]]]()
        val serializer = SparkEnv.get.closureSerializer.newInstance()
        var serializeBytes: Array[Byte] = null
        if (carbonLoadModel.isLoadWithoutConverterWithoutReArrangeStep) {
          // based on RDD[InternalRow]
          val partitionIterator = firstParent[DataLoadPartitionWrap[InternalRow]].iterator(theSplit,
            context)
          while (partitionIterator.hasNext) {
            val value = partitionIterator.next()
            if (serializeBytes == null) {
              serializeBytes = serializer.serialize[RDD[InternalRow]](value.rdd).array()
            }
            recordReaders +=
            new LazyRddInternalRowIterator(serializer, serializeBytes, value.partition,
              carbonLoadModel, context)
          }
        } else {
          val partitionIterator = firstParent[DataLoadPartitionWrap[Row]].iterator(theSplit,
            context)
          while (partitionIterator.hasNext) {
            val value = partitionIterator.next()
            if (serializeBytes == null) {
              serializeBytes = serializer.serialize[RDD[Row]](value.rdd).array()
            }
            recordReaders += new LazyRddIterator(serializer, serializeBytes, value.partition,
              carbonLoadModel, context)
          }
        }
        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null,
          loadMetadataDetails)
        // Initialize to set carbon properties
        loader.initialize()
        val executor = new DataLoadExecutor
        // in case of success, failure or cancelation clear memory and stop execution
        context
          .addTaskCompletionListener(new InsertTaskCompletionListener(executor, executionErrors))
        executor.execute(model, loader.storeLocation, recordReaders.toArray)
      } catch {
        case e: NoRetryException =>
          loadMetadataDetails.setSegmentStatus(SegmentStatus.LOAD_PARTIAL_SUCCESS)
          executionErrors.failureCauses = FailureCauses.BAD_RECORDS
          executionErrors.errorMsg = e.getMessage
          logInfo("Bad Record Found")
        case e: Exception =>
          loadMetadataDetails.setSegmentStatus(SegmentStatus.LOAD_FAILURE)
          logInfo("DataLoad failure", e)
          LOGGER.error(e)
          throw e
      } finally {
        // clean up the folders and files created locally for data load operation
        TableProcessingOperations.deleteLocalDataLoadFolderLocation(model, false, false)
        // in case of failure the same operation will be re-tried several times.
        // So print the data load statistics only in case of non failure case
        if (SegmentStatus.LOAD_FAILURE != loadMetadataDetails.getSegmentStatus) {
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance
            .printStatisticsInfo(CarbonTablePath.DEPRECATED_PARTITION_ID)
        }
      }
      var finished = false

      override def hasNext: Boolean = !finished

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, (loadMetadataDetails, executionErrors))
      }
    }
    iter
  }
  override protected def internalGetPartitions: Array[Partition] = firstParent[Row].partitions
}

/**
 * This class wrap Scala's Iterator to Java's Iterator.
 * It also convert all columns to string data to use csv data loading flow.
 *
 * @param rddIter
 * @param carbonLoadModel
 * @param context
 */
class NewRddIterator(rddIter: Iterator[Row],
    carbonLoadModel: CarbonLoadModel,
    context: TaskContext) extends CarbonIterator[Array[AnyRef]] {

  private val timeStampformatString = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  private val timeStampFormat = new SimpleDateFormat(timeStampformatString)
  private val dateFormatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
    .CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  private val dateFormat = new SimpleDateFormat(dateFormatString)
  private val complexDelimiters = carbonLoadModel.getComplexDelimiters
  private val serializationNullFormat =
    carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
  import scala.collection.JavaConverters._
  private val isVarcharTypeMapping =
    carbonLoadModel.getCarbonDataLoadSchema
      .getCarbonTable.getCreateOrderColumn.asScala.map(_.getDataType == DataTypes.VARCHAR)
  private val isComplexTypeMapping =
    carbonLoadModel.getCarbonDataLoadSchema
      .getCarbonTable.getCreateOrderColumn.asScala.map(_.isComplex())
  def hasNext: Boolean = rddIter.hasNext

  def next: Array[AnyRef] = {
    val row = rddIter.next()
    val columns = new Array[AnyRef](row.length)
    val len = columns.length
    var i = 0
    while (i < len) {
      columns(i) = CarbonScalaUtil.getString(row, i, carbonLoadModel, serializationNullFormat,
        complexDelimiters, timeStampFormat, dateFormat,
        isVarcharType = i < isVarcharTypeMapping.size && isVarcharTypeMapping(i),
        isComplexType = i < isComplexTypeMapping.size && isComplexTypeMapping(i))
      i += 1
    }
    columns
  }

  override def initialize(): Unit = {
    SparkUtil.setTaskContext(context)
  }

}

/**
 * LazyRddIterator invoke rdd.iterator method when invoking hasNext method.
 * @param serializer
 * @param serializeBytes
 * @param partition
 * @param carbonLoadModel
 * @param context
 */
class LazyRddIterator(serializer: SerializerInstance,
    serializeBytes: Array[Byte],
    partition: Partition,
    carbonLoadModel: CarbonLoadModel,
    context: TaskContext) extends CarbonIterator[Array[AnyRef]] {

  private val timeStampformatString = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  private val timeStampFormat = new SimpleDateFormat(timeStampformatString)
  private val dateFormatString = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  private val dateFormat = new SimpleDateFormat(dateFormatString)
  private val complexDelimiters = carbonLoadModel.getComplexDelimiters
  private val serializationNullFormat =
    carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
  // the order of fields in dataframe and createTable may be different, here we need to know whether
  // each fields in dataframe is Varchar or not.
  import scala.collection.JavaConverters._
  private val isVarcharTypeMapping = {
    val col2VarcharType = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
          .getCreateOrderColumn().asScala
      .map(c => c.getColName -> (c.getDataType == DataTypes.VARCHAR)).toMap
    carbonLoadModel.getCsvHeaderColumns.map(c => {
      val r = col2VarcharType.get(c.toLowerCase)
      r.isDefined && r.get
    })
  }

  private var rddIter: Iterator[Row] = null
  private var uninitialized = true
  private var closed = false

  def hasNext: Boolean = {
    if (uninitialized) {
      uninitialized = false
      rddIter = serializer.deserialize[RDD[Row]](ByteBuffer.wrap(serializeBytes))
        .iterator(partition, context)
    }
    if (closed) {
      false
    } else {
      rddIter.hasNext
    }
  }

  def next: Array[AnyRef] = {
    val row = rddIter.next()
    val columns = new Array[AnyRef](row.length)
    for (i <- 0 until columns.length) {
      columns(i) = CarbonScalaUtil.getString(row, i, carbonLoadModel, serializationNullFormat,
        complexDelimiters, timeStampFormat, dateFormat,
        isVarcharType = i < isVarcharTypeMapping.size && isVarcharTypeMapping(i))
    }
    columns
  }

  override def initialize(): Unit = {
    SparkUtil.setTaskContext(context)
  }

  override def close(): Unit = {
    closed = true
    rddIter = null
  }

}

/**
 * LazyRddInternalRowIterator invoke rdd.iterator method when invoking hasNext method.
 * @param serializer
 * @param serializeBytes
 * @param partition
 * @param carbonLoadModel
 * @param context
 */
class LazyRddInternalRowIterator(serializer: SerializerInstance,
    serializeBytes: Array[Byte],
    partition: Partition,
    carbonLoadModel: CarbonLoadModel,
    context: TaskContext) extends CarbonIterator[Array[AnyRef]] {

  private var rddIter: Iterator[InternalRow] = null
  private var uninitialized = true
  private var closed = false

  val dataTypes = Util
    .convertToSparkSchemaFromColumnSchema(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
      false)
    .fields
    .map(field => field.dataType)
    .toSeq

  def hasNext: Boolean = {
    if (uninitialized) {
      uninitialized = false
      rddIter = serializer.deserialize[RDD[InternalRow]](ByteBuffer.wrap(serializeBytes))
        .iterator(partition, context)
    }
    if (closed) {
      false
    } else {
      rddIter.hasNext
    }
  }

  def next: Array[AnyRef] = {
    CommonUtil.getObjectArrayFromInternalRowAndConvertComplexType(rddIter.next(),
      dataTypes,
      dataTypes.length)
  }

  override def initialize(): Unit = {
    SparkUtil.setTaskContext(context)
  }

  override def close(): Unit = {
    closed = true
    rddIter = null
  }

}
