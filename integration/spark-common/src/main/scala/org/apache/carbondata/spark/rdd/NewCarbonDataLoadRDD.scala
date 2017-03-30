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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SerializableWritable, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionWrap, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.common.logging.impl.StandardLogService
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory}
import org.apache.carbondata.processing.csvload.BlockDetails
import org.apache.carbondata.processing.csvload.CSVInputFormat
import org.apache.carbondata.processing.csvload.CSVRecordReaderIterator
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.DataLoadExecutor
import org.apache.carbondata.processing.newflow.exception.BadRecordFoundException
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.{CarbonQueryUtil, CarbonScalaUtil, CommonUtil}

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

/**
 * This partition class use to split by TableSplit
 *
 */
class CarbonTableSplitPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit,
    val blocksDetails: Array[BlockDetails])
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)
  val partitionBlocksDetail = blocksDetails

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

class SparkPartitionLoader(model: CarbonLoadModel,
    splitIndex: Int,
    storePath: String,
    loadCount: String,
    loadMetadataDetails: LoadMetadataDetails) {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  var storeLocation: String = ""

  def initialize(): Unit = {
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties")
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance.initPartitonInfo(model.getPartitionId)
    CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true")
    CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1")
    CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true")
    CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true")
    CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true")
    CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000")
    CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false")
    CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000")

    // this property is used to determine whether temp location for carbon is inside
    // container temp dir or is yarn application directory.
    val carbonUseLocalDir = CarbonProperties.getInstance()
      .getProperty("carbon.use.local.dir", "false")
    if (carbonUseLocalDir.equalsIgnoreCase("true")) {
      val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
      if (null != storeLocations && storeLocations.nonEmpty) {
        storeLocation = storeLocations(Random.nextInt(storeLocations.length))
      }
      if (storeLocation == null) {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
    } else {
      storeLocation = System.getProperty("java.io.tmpdir")
    }
    storeLocation = storeLocation + '/' + System.nanoTime() + '/' + splitIndex
  }

}

/**
 * It loads the data to carbon using @AbstractDataLoadProcessorStep
 */
class NewCarbonDataLoadRDD[K, V](
    sc: SparkContext,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    loadCount: Integer,
    blocksGroupBy: Array[(String, Array[BlockDetails])],
    isTableSplitPartition: Boolean)
  extends RDD[(K, V)](sc, Nil) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast =
    sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration))

  override def getPartitions: Array[Partition] = {
    if (isTableSplitPartition) {
      // for table split partition
      var splits: Array[TableSplit] = null

      if (carbonLoadModel.isDirectLoad) {
        splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath)
      } else {
        splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
          carbonLoadModel.getTableName, null)
      }

      splits.zipWithIndex.map { s =>
        // filter the same partition unique id, because only one will match, so get 0 element
        val blocksDetails: Array[BlockDetails] = blocksGroupBy.filter(p =>
          p._1 == s._1.getPartition.getUniqueID)(0)._2
        new CarbonTableSplitPartition(id, s._2, s._1, blocksDetails)
      }
    } else {
      // for node partition
      blocksGroupBy.zipWithIndex.map { b =>
        new CarbonNodePartition(id, b._2, b._1._1, b._1._2)
      }
    }
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      var partitionID = "0"
      val loadMetadataDetails = new LoadMetadataDetails()
      var model: CarbonLoadModel = _
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)

        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        val preFetch = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
          .USE_PREFETCH_WHILE_LOADING, CarbonCommonConstants.USE_PREFETCH_WHILE_LOADING_DEFAULT)
        carbonLoadModel.setPreFetch(preFetch.toBoolean)
        val recordReaders = getInputIterators
        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null,
          String.valueOf(loadCount),
          loadMetadataDetails)
        // Intialize to set carbon properties
        loader.initialize()
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
        new DataLoadExecutor().execute(model,
          loader.storeLocation,
          recordReaders)
      } catch {
        case e: BadRecordFoundException =>
          loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
          logInfo("Bad Record Found")
        case e: Exception =>
          logInfo("DataLoad failure", e)
          LOGGER.error(e)
          throw e
      }

      def getInputIterators: Array[CarbonIterator[Array[AnyRef]]] = {
        val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, theSplit.index, 0)
        var configuration: Configuration = confBroadcast.value.value
        if (configuration == null) {
          configuration = new Configuration()
        }
        CommonUtil.configureCSVInputFormat(configuration, carbonLoadModel)
        val hadoopAttemptContext = new TaskAttemptContextImpl(configuration, attemptId)
        val format = new CSVInputFormat
        if (isTableSplitPartition) {
          // for table split partition
          val split = theSplit.asInstanceOf[CarbonTableSplitPartition]
          logInfo("Input split: " + split.serializableHadoopSplit.value)
          carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
          if (carbonLoadModel.isDirectLoad) {
            model = carbonLoadModel.getCopyWithPartition(
                split.serializableHadoopSplit.value.getPartition.getUniqueID,
                split.serializableHadoopSplit.value.getPartition.getFilesPath,
                carbonLoadModel.getCsvHeader, carbonLoadModel.getCsvDelimiter)
          } else {
            model = carbonLoadModel.getCopyWithPartition(
                split.serializableHadoopSplit.value.getPartition.getUniqueID)
          }
          partitionID = split.serializableHadoopSplit.value.getPartition.getUniqueID

          StandardLogService.setThreadName(partitionID, null)
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordPartitionBlockMap(
              partitionID, split.partitionBlocksDetail.length)
          val readers =
          split.partitionBlocksDetail.map(format.createRecordReader(_, hadoopAttemptContext))
          readers.zipWithIndex.map { case (reader, index) =>
            new CSVRecordReaderIterator(reader,
              split.partitionBlocksDetail(index),
              hadoopAttemptContext)
          }
        } else {
          // for node partition
          val split = theSplit.asInstanceOf[CarbonNodePartition]
          logInfo("Input split: " + split.serializableHadoopSplit)
          logInfo("The Block Count in this node :" + split.nodeBlocksDetail.length)
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordHostBlockMap(
              split.serializableHadoopSplit, split.nodeBlocksDetail.length)
          val blocksID = gernerateBlocksID
          carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
          if (carbonLoadModel.isDirectLoad) {
            val filelist: java.util.List[String] = new java.util.ArrayList[String](
                CarbonCommonConstants.CONSTANT_SIZE_TEN)
            CarbonQueryUtil.splitFilePath(carbonLoadModel.getFactFilePath, filelist, ",")
            model = carbonLoadModel.getCopyWithPartition(partitionID, filelist,
                carbonLoadModel.getCsvHeader, carbonLoadModel.getCsvDelimiter)
          } else {
            model = carbonLoadModel.getCopyWithPartition(partitionID)
          }
          StandardLogService.setThreadName(blocksID, null)
          val readers =
            split.nodeBlocksDetail.map(format.createRecordReader(_, hadoopAttemptContext))
          readers.zipWithIndex.map { case (reader, index) =>
            new CSVRecordReaderIterator(reader, split.nodeBlocksDetail(index), hadoopAttemptContext)
          }
        }
      }

      /**
       * generate blocks id
       *
       * @return
       */
      def gernerateBlocksID: String = {
        if (isTableSplitPartition) {
          carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName + "_" +
          theSplit.asInstanceOf[CarbonTableSplitPartition].serializableHadoopSplit.value
            .getPartition.getUniqueID + "_" + UUID.randomUUID()
        } else {
          carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName + "_" +
          UUID.randomUUID()
        }
      }

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, loadMetadataDetails)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (isTableSplitPartition) {
      val theSplit = split.asInstanceOf[CarbonTableSplitPartition]
      val location = theSplit.serializableHadoopSplit.value.getLocations.asScala
      location
    } else {
      val theSplit = split.asInstanceOf[CarbonNodePartition]
      val firstOptionLocation: Seq[String] = List(theSplit.serializableHadoopSplit)
      logInfo("Preferred Location for split : " + firstOptionLocation.head)
      val blockMap = new util.LinkedHashMap[String, Integer]()
      val tableBlocks = theSplit.blocksDetails
      tableBlocks.foreach { tableBlock =>
        tableBlock.getLocations.foreach { location =>
          if (!firstOptionLocation.exists(location.equalsIgnoreCase(_))) {
            val currentCount = blockMap.get(location)
            if (currentCount == null) {
              blockMap.put(location, 1)
            } else {
              blockMap.put(location, currentCount + 1)
            }
          }
        }
      }

      val sortedList = blockMap.entrySet().asScala.toSeq.sortWith { (nodeCount1, nodeCount2) =>
        nodeCount1.getValue > nodeCount2.getValue
      }

      val sortedNodesList = sortedList.map(nodeCount => nodeCount.getKey).take(2)
      firstOptionLocation ++ sortedNodesList
    }
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
                                   loadCount: Integer,
                                   tableCreationTime: Long,
                                   schemaLastUpdatedTime: Long,
                                   prev: DataLoadCoalescedRDD[Row]) extends RDD[(K, V)](prev) {


  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      val partitionID = "0"
      val loadMetadataDetails = new LoadMetadataDetails()
      val model: CarbonLoadModel = carbonLoadModel
      val uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {

        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)
        carbonLoadModel.setPartitionId(partitionID)
        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        carbonLoadModel.setPreFetch(false)

        val recordReaders = mutable.Buffer[CarbonIterator[Array[AnyRef]]]()
        val partitionIterator = firstParent[DataLoadPartitionWrap[Row]].iterator(theSplit, context)
        val serializer = SparkEnv.get.closureSerializer.newInstance()
        var serializeBuffer: ByteBuffer = null
        while(partitionIterator.hasNext) {
          val value = partitionIterator.next()
          val newInstance = {
            if (serializeBuffer == null) {
              serializeBuffer = serializer.serialize[RDD[Row]](value.rdd)
            }
            serializeBuffer.rewind()
            serializer.deserialize[RDD[Row]](serializeBuffer)
          }
          recordReaders += new NewRddIterator(newInstance.iterator(value.partition, context),
              carbonLoadModel,
              context)
        }

        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null,
          String.valueOf(loadCount),
          loadMetadataDetails)
        // Intialize to set carbon properties
        loader.initialize()

        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
        new DataLoadExecutor().execute(model, loader.storeLocation, recordReaders.toArray)

      } catch {
        case e: BadRecordFoundException =>
          loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
          logInfo("Bad Record Found")
        case e: Exception =>
          logInfo("DataLoad failure", e)
          LOGGER.error(e)
          throw e
      }
      var finished = false

      override def hasNext: Boolean = !finished

      override def next(): (K, V) = {
        finished = true
        result.getKey(uniqueLoadStatusId, loadMetadataDetails)
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

  val timeStampformatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
    .CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  val timeStampFormat = new SimpleDateFormat(timeStampformatString)
  val dateFormatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
    .CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
  val dateFormat = new SimpleDateFormat(dateFormatString)
  val delimiterLevel1 = carbonLoadModel.getComplexDelimiterLevel1
  val delimiterLevel2 = carbonLoadModel.getComplexDelimiterLevel2
  val serializationNullFormat =
    carbonLoadModel.getSerializationNullFormat.split(CarbonCommonConstants.COMMA, 2)(1)
  def hasNext: Boolean = rddIter.hasNext

  def next: Array[AnyRef] = {
    val row = rddIter.next()
    val columns = new Array[AnyRef](row.length)
    for (i <- 0 until columns.length) {
      columns(i) = CarbonScalaUtil.getString(row.get(i), serializationNullFormat,
        delimiterLevel1, delimiterLevel2, timeStampFormat, dateFormat)
    }
    columns
  }

  override def initialize: Unit = {
    SparkUtil.setTaskContext(context)
  }

}
