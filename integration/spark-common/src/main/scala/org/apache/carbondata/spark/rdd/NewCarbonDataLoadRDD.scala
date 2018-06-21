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

import java.io._
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SerializableWritable, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionWrap, RDD}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.common.logging.impl.StandardLogService
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory, ThreadLocalTaskInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.{DataLoadExecutor, FailureCauses, TableProcessingOperations}
import org.apache.carbondata.processing.loading.csvinput.{BlockDetails, CSVInputFormat, CSVRecordReaderIterator}
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonQueryUtil
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil, Util}

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  @transient
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private def writeObject(out: ObjectOutputStream): Unit =
    try {
      out.defaultWriteObject()
      value.write(out)
    } catch {
      case e: IOException =>
        LOGGER.error(e, "Exception encountered")
        throw e
      case NonFatal(e) =>
        LOGGER.error(e, "Exception encountered")
        throw new IOException(e)
    }


  private def readObject(in: ObjectInputStream): Unit =
    try {
      value = new Configuration(false)
      value.readFields(in)
    } catch {
      case e: IOException =>
        LOGGER.error(e, "Exception encountered")
        throw e
      case NonFatal(e) =>
        LOGGER.error(e, "Exception encountered")
        throw new IOException(e)
    }
}

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
    storePath: String) {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  var storeLocation: Array[String] = Array[String]()

  def initialize(): Unit = {
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties")
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance.initPartitionInfo(
      CarbonTablePath.DEPRECATED_PATITION_ID)
    CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true")
    CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1")
    CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true")
    CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true")
    CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true")
    CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false")

    // this property is used to determine whether temp location for carbon is inside
    // container temp dir or is yarn application directory.
    val isCarbonUseLocalDir = CarbonProperties.getInstance()
      .getProperty("carbon.use.local.dir", "false").equalsIgnoreCase("true")

    val isCarbonUseMultiDir = CarbonProperties.getInstance().isUseMultiTempDir

    if (isCarbonUseLocalDir) {
      val yarnStoreLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)

      if (!isCarbonUseMultiDir && null != yarnStoreLocations && yarnStoreLocations.nonEmpty) {
        // use single dir
        storeLocation = storeLocation :+
            (yarnStoreLocations(Random.nextInt(yarnStoreLocations.length)) + tmpLocationSuffix)
        if (storeLocation == null || storeLocation.isEmpty) {
          storeLocation = storeLocation :+
              (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
        }
      } else {
        // use all the yarn dirs
        storeLocation = yarnStoreLocations.map(_ + tmpLocationSuffix)
      }
    } else {
      storeLocation = storeLocation :+ (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
    }
    LOGGER.info("Temp location for loading data: " + storeLocation.mkString(","))
  }

  private def tmpLocationSuffix = File.separator + "carbon" + System.nanoTime() + "_" + splitIndex
}

/**
 * It loads the data to carbon using @AbstractDataLoadProcessorStep
 */
class NewCarbonDataLoadRDD[K, V](
    sc: SparkContext,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    blocksGroupBy: Array[(String, Array[BlockDetails])],
    @transient hadoopConf: Configuration)
  extends CarbonRDD[(K, V)](sc, Nil, hadoopConf) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  private val confBytes = {
    val bao = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bao)
    hadoopConf.write(oos)
    oos.close()
    CompressorFactory.getInstance().getCompressor.compressByte(bao.toByteArray)
  }

  override def getPartitions: Array[Partition] = {
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
      var status = SegmentStatus.SUCCESS
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      var model: CarbonLoadModel = _
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {
        val preFetch = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
          .USE_PREFETCH_WHILE_LOADING, CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING_DEFAULT)
        carbonLoadModel.setPreFetch(preFetch.toBoolean)
        val recordReaders = getInputIterators
        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null)
        // Initialize to set carbon properties
        loader.initialize()
        val executor = new DataLoadExecutor()
        // in case of success, failure or cancelation clear memory and stop execution
        context.addTaskCompletionListener { context => executor.close()
          CommonUtil.clearUnsafeMemory(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)}
        executor.execute(model,
          loader.storeLocation,
          recordReaders)
      } catch {
        case e: NoRetryException =>
          status = SegmentStatus.LOAD_PARTIAL_SUCCESS
          executionErrors.failureCauses = FailureCauses.BAD_RECORDS
          executionErrors.errorMsg = e.getMessage
          logInfo("Bad Record Found")
        case e: Exception =>
          status = SegmentStatus.LOAD_FAILURE
          logInfo("DataLoad failure", e)
          LOGGER.error(e)
          throw e
      } finally {
        // clean up the folders and files created locally for data load operation
        TableProcessingOperations.deleteLocalDataLoadFolderLocation(model, false, false)
        // in case of failure the same operation will be re-tried several times.
        // So print the data load statistics only in case of non failure case
        if (SegmentStatus.LOAD_FAILURE != status) {
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance
            .printStatisticsInfo(CarbonTablePath.DEPRECATED_PATITION_ID)
        }
      }

      def getInputIterators: Array[CarbonIterator[Array[AnyRef]]] = {
        val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, theSplit.index, 0)
        var configuration: Configuration = getConf
        if (configuration == null) {
          configuration = new Configuration()
        }
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
        StandardLogService.setThreadName(StandardLogService
          .getPartitionID(model.getCarbonDataLoadSchema.getCarbonTable.getTableUniqueName)
          , ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId + "")
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
      def gernerateBlocksID: String = {
        carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName + "_" +
        UUID.randomUUID()
      }

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, (status, executionErrors))
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
    sc: SparkContext,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    prev: DataLoadCoalescedRDD[Row],
    @transient hadoopConf: Configuration) extends CarbonRDD[(K, V)](prev) {

  private val confBytes = {
    val bao = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bao)
    hadoopConf.write(oos)
    oos.close()
    CompressorFactory.getInstance().getCompressor.compressByte(bao.toByteArray)
  }

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val hadoopConf = getConf
    CarbonInputFormatUtil.setS3Configurations(hadoopConf)
    val iter = new Iterator[(K, V)] {
      var status = SegmentStatus.SUCCESS
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      val model: CarbonLoadModel = carbonLoadModel
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {

        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        carbonLoadModel.setPreFetch(false)

        val recordReaders = mutable.Buffer[CarbonIterator[Array[AnyRef]]]()
        val partitionIterator = firstParent[DataLoadPartitionWrap[Row]].iterator(theSplit, context)
        val serializer = SparkEnv.get.closureSerializer.newInstance()
        var serializeBytes: Array[Byte] = null
        while(partitionIterator.hasNext) {
          val value = partitionIterator.next()
          if (serializeBytes == null) {
            serializeBytes = serializer.serialize[RDD[Row]](value.rdd).array()
          }
          recordReaders += new LazyRddIterator(serializer, serializeBytes, value.partition,
              carbonLoadModel, context)
        }
        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null)
        // Initialize to set carbon properties
        loader.initialize()
        val executor = new DataLoadExecutor
        // in case of success, failure or cancelation clear memory and stop execution
        context.addTaskCompletionListener { context => executor.close()
          CommonUtil.clearUnsafeMemory(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)}
        executor.execute(model, loader.storeLocation, recordReaders.toArray)
      } catch {
        case e: NoRetryException =>
          status = SegmentStatus.LOAD_PARTIAL_SUCCESS
          executionErrors.failureCauses = FailureCauses.BAD_RECORDS
          executionErrors.errorMsg = e.getMessage
          logInfo("Bad Record Found")
        case e: Exception =>
          status = SegmentStatus.LOAD_FAILURE
          logInfo("DataLoad failure", e)
          LOGGER.error(e)
          throw e
      } finally {
        // clean up the folders and files created locally for data load operation
        TableProcessingOperations.deleteLocalDataLoadFolderLocation(model, false, false)
        // in case of failure the same operation will be re-tried several times.
        // So print the data load statistics only in case of non failure case
        if (SegmentStatus.LOAD_FAILURE != status) {
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance
            .printStatisticsInfo(CarbonTablePath.DEPRECATED_PATITION_ID)
        }
      }
      var finished = false

      override def hasNext: Boolean = !finished

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, (status, executionErrors))
      }
    }
    iter
  }
  override protected def getPartitions: Array[Partition] = firstParent[Row].partitions
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
  private val delimiterLevel1 = carbonLoadModel.getComplexDelimiterLevel1
  private val delimiterLevel2 = carbonLoadModel.getComplexDelimiterLevel2
  private val serializationNullFormat =
    carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
  import scala.collection.JavaConverters._
  private val isVarcharTypeMapping =
    carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getCreateOrderColumn(
      carbonLoadModel.getTableName).asScala.map(_.getDataType == DataTypes.VARCHAR)
  def hasNext: Boolean = rddIter.hasNext

  def next: Array[AnyRef] = {
    val row = rddIter.next()
    val columns = new Array[AnyRef](row.length)
    for (i <- 0 until columns.length) {
      columns(i) = CarbonScalaUtil.getString(row.get(i), serializationNullFormat,
        delimiterLevel1, delimiterLevel2, timeStampFormat, dateFormat,
        isVarcharType = i < isVarcharTypeMapping.size && isVarcharTypeMapping(i))
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
  private val delimiterLevel1 = carbonLoadModel.getComplexDelimiterLevel1
  private val delimiterLevel2 = carbonLoadModel.getComplexDelimiterLevel2
  private val serializationNullFormat =
    carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
  import scala.collection.JavaConverters._
  private val isVarcharTypeMapping =
    carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getCreateOrderColumn(
      carbonLoadModel.getTableName).asScala.map(_.getDataType == DataTypes.VARCHAR)

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
      columns(i) = CarbonScalaUtil.getString(row.get(i), serializationNullFormat,
        delimiterLevel1, delimiterLevel2, timeStampFormat, dateFormat,
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

/*
 *  It loads the data  to carbon from RDD for partition table
 *  @see org.apache.carbondata.processing.newflow.DataLoadExecutor
 */
class PartitionTableDataLoaderRDD[K, V](
    sc: SparkContext,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    prev: RDD[Row]) extends CarbonRDD[(K, V)](prev) {

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      var status = SegmentStatus.SUCCESS
      val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
      val model: CarbonLoadModel = carbonLoadModel
      val carbonTable = model.getCarbonDataLoadSchema.getCarbonTable
      val partitionInfo = carbonTable.getPartitionInfo(carbonTable.getTableName)
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {

        carbonLoadModel.setTaskNo(String.valueOf(partitionInfo.getPartitionId(theSplit.index)))
        carbonLoadModel.setPreFetch(false)
        val recordReaders = Array[CarbonIterator[Array[AnyRef]]] {
          new NewRddIterator(firstParent[Row].iterator(theSplit, context), carbonLoadModel, context)
        }

        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null)
        // Initialize to set carbon properties
        loader.initialize()
        val executor = new DataLoadExecutor
        // in case of success, failure or cancelation clear memory and stop execution
        context.addTaskCompletionListener { context => executor.close()
          CommonUtil.clearUnsafeMemory(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)}
        executor.execute(model, loader.storeLocation, recordReaders)
      } catch {
        case e: NoRetryException =>
          status = SegmentStatus.LOAD_PARTIAL_SUCCESS
          executionErrors.failureCauses = FailureCauses.BAD_RECORDS
          executionErrors.errorMsg = e.getMessage
          logInfo("Bad Record Found")
        case e: Exception =>
          status = SegmentStatus.LOAD_FAILURE
          logInfo("DataLoad For Partition Table failure", e)
          LOGGER.error(e)
          throw e
      } finally {
        // clean up the folders and files created locally for data load operation
        TableProcessingOperations.deleteLocalDataLoadFolderLocation(model, false, false)
        // in case of failure the same operation will be re-tried several times.
        // So print the data load statistics only in case of non failure case
        if (SegmentStatus.LOAD_FAILURE != status) {
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance
            .printStatisticsInfo(CarbonTablePath.DEPRECATED_PATITION_ID)
        }
      }
      var finished = false
      override def hasNext: Boolean = !finished

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, (status, executionErrors))
      }
    }
    iter
  }

  override protected def getPartitions: Array[Partition] = firstParent[Row].partitions

}
