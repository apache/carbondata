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

import java.lang.Long
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.{Partition, SerializableWritable, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{DataLoadCoalescedRDD, DataLoadPartitionWrap, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.common.logging.impl.StandardLogService
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory}
import org.apache.carbondata.processing.constants.DataProcessorConstants
import org.apache.carbondata.processing.csvreaderstep.{BlockDetails, RddInputUtils}
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.graphgenerator.GraphGenerator
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.{CarbonQueryUtil, CarbonScalaUtil}

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
    splitIndex: Int,
    storePath: String,
    kettleHomePath: String,
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

  def run(): Unit = {
    try {
      CarbonLoaderUtil.executeGraph(model, storeLocation, storePath,
        kettleHomePath)
      loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
    } catch {
      case e: DataLoadingException => if (e.getErrorCode ==
                                          DataProcessorConstants.BAD_REC_FOUND) {
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
        LOGGER.info("Bad Record Found")
      } else {
        throw e
      }
      case e: Exception =>
        throw e
    } finally {
      // delete temp location data
      try {
        val isCompaction = false
        CarbonLoaderUtil.deleteLocalDataLoadFolderLocation(model, isCompaction)
      } catch {
        case e: Exception =>
          LOGGER.error(e, "Failed to delete local data")
      }
      if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(
        loadMetadataDetails.getLoadStatus)) {
        if (CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
          .equals(loadMetadataDetails.getLoadStatus)) {
          LOGGER.info("DataLoad complete")
          LOGGER.info("Data Load partially successful with LoadCount:" + loadCount)
        } else {
          LOGGER.info("DataLoad complete")
          LOGGER.info("Data Loaded successfully with LoadCount:" + loadCount)
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance.printStatisticsInfo(
            model.getPartitionId)
        }
      }
    }
  }
}

/**
 * Use this RDD class to load csv data file
 *
 * @param sc                    The SparkContext to associate the RDD with.
 * @param result                Output result
 * @param carbonLoadModel       Carbon load model which contain the load info
 * @param storePath             The store location
 * @param kettleHomePath        The kettle home path
 * @param columnar             whether it is columnar
 * @param loadCount             Current load count
 * @param tableCreationTime     Time of creating table
 * @param schemaLastUpdatedTime Time of last schema update
 * @param blocksGroupBy         Blocks Array which is group by partition or host
 * @param isTableSplitPartition Whether using table split partition
 * @tparam K Class of the key associated with the Result.
 * @tparam V Class of the value associated with the Result.
 */
class DataFileLoaderRDD[K, V](
    sc: SparkContext,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    storePath: String,
    kettleHomePath: String,
    columnar: Boolean,
    loadCount: Integer,
    tableCreationTime: Long,
    schemaLastUpdatedTime: Long,
    blocksGroupBy: Array[(String, Array[BlockDetails])],
    isTableSplitPartition: Boolean) extends RDD[(K, V)](sc, Nil) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    if (isTableSplitPartition) {
      // for table split partition
      var splits = Array[TableSplit]()
      if (carbonLoadModel.isDirectLoad) {
        splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath)
      } else {
        splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
          carbonLoadModel.getTableName, null)
      }

      splits.zipWithIndex.map { case (split, index) =>
        // filter the same partition unique id, because only one will match, so get 0 element
        val blocksDetails: Array[BlockDetails] = blocksGroupBy.filter { case (uniqueId, _) =>
          uniqueId == split.getPartition.getUniqueID
        }(0)._2
        new CarbonTableSplitPartition(id, index, split, blocksDetails)
      }
    } else {
      // for node partition
      blocksGroupBy.zipWithIndex.map { case ((uniqueId, blockDetails), index) =>
        new CarbonNodePartition(id, index, uniqueId, blockDetails)
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
      val uniqueLoadStatusId = carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE +
          theSplit.index
      try {
        loadMetadataDetails.setPartitionCount(partitionID)
        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        setModelAndBlocksInfo()
        val loader = new SparkPartitionLoader(model, theSplit.index, storePath,
          kettleHomePath, String.valueOf(loadCount), loadMetadataDetails)
        loader.initialize
        if (model.isRetentionRequest) {
          recreateAggregationTableForRetention
        } else if (model.isAggLoadRequest) {
          loadMetadataDetails.setLoadStatus(createManualAggregateTable)
        } else {
          loader.run()
        }
      } catch {
        case e: Exception =>
          logInfo("DataLoad failure")
          LOGGER.error(e)
          throw e
      }

      def setModelAndBlocksInfo(): Unit = {
        if (isTableSplitPartition) {
          // for table split partition
          val split = theSplit.asInstanceOf[CarbonTableSplitPartition]
          logInfo("Input split: " + split.serializableHadoopSplit.value)
          val blocksID = gernerateBlocksID
          carbonLoadModel.setBlocksID(blocksID)
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
          // get this partition data blocks and put it to global static map
          GraphGenerator.blockInfo.put(blocksID, split.partitionBlocksDetail)
          StandardLogService.setThreadName(partitionID, null)
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordPartitionBlockMap(
            partitionID, split.partitionBlocksDetail.length)
        } else {
          // for node partition
          val split = theSplit.asInstanceOf[CarbonNodePartition]
          logInfo("Input split: " + split.serializableHadoopSplit)
          logInfo("The Block Count in this node: " + split.nodeBlocksDetail.length)
          CarbonTimeStatisticsFactory.getLoadStatisticsInstance.recordHostBlockMap(
            split.serializableHadoopSplit, split.nodeBlocksDetail.length)
          val blocksID = gernerateBlocksID
          carbonLoadModel.setBlocksID(blocksID)
          carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
          // set this node blocks info to global static map
          GraphGenerator.blockInfo.put(blocksID, split.nodeBlocksDetail)
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

      def checkAndLoadAggregationTable: String = {
        val schema = model.getCarbonDataLoadSchema
        val aggTables = schema.getCarbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.asScala.toArray
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + loadCount
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders)
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          var listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
          listOfAllLoadFolders = CarbonLoaderUtil
            .addNewSliceNameToList(newSlice, listOfAllLoadFolders)
          val copyListOfLoadFolders = listOfLoadFolders.asScala.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.asScala.toList
          loadTableSlices(listOfAllLoadFolders, details)
          val loadFolders = Array[String]()
          loadMetadataDetails.setLoadStatus(iterateOverAggTables(aggTables,
            copyListOfLoadFolders.asJava, copyListOfUpdatedLoadFolders.asJava, loadFolders))
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(
            loadMetadataDetails.getLoadStatus)) {
            // remove the current slice from memory not the table
            CarbonLoaderUtil
              .removeSliceFromMemory(model.getDatabaseName, model.getTableName, newSlice)
            logInfo(s"Aggregate table creation failed")
          } else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        loadMetadataDetails.getLoadStatus
      }

      def loadTableSlices(listOfAllLoadFolders: java.util.List[String],
          loadMetadataDetails: Array[LoadMetadataDetails]) = {
        CarbonProperties.getInstance().addProperty("carbon.cache.used", "false")
        // TODO: Implement it
      }

      def createManualAggregateTable: String = {
        val details = model.getLoadMetadataDetails.asScala.toArray
        val listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
        val listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
        val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
        loadTableSlices(listOfAllLoadFolders, details)
        val loadFolders = Array[String]()
        val aggTable = model.getAggTableName
        loadMetadataDetails.setLoadStatus(loadAggregationTable(listOfLoadFolders,
          listOfUpdatedLoadFolders, loadFolders))
        if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(
          loadMetadataDetails.getLoadStatus)) {
          logInfo(s"Aggregate table creation failed :: $aggTable")
        } else {
          logInfo(s"Aggregate table creation successfull :: $aggTable")
        }
        loadMetadataDetails.getLoadStatus
      }

      def recreateAggregationTableForRetention = {
        val schema = model.getCarbonDataLoadSchema
        val aggTables = schema.getCarbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.asScala.toArray
          val listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          val listOfAllLoadFolder = CarbonQueryUtil.getListOfSlices(details)
          loadTableSlices(listOfAllLoadFolder, details)
          val loadFolders = Array[String]()
          iterateOverAggTables(aggTables, listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
        }
      }

      // TODO Aggregate table needs to be handled
      def iterateOverAggTables(aggTables: java.util.List[String],
          listOfLoadFolders: java.util.List[String],
          listOfUpdatedLoadFolders: java.util.List[String],
          loadFolders: Array[String]): String = {
        model.setAggLoadRequest(true)
        aggTables.asScala.foreach { aggTable =>
          model.setAggTableName(aggTable)
          loadMetadataDetails.setLoadStatus(loadAggregationTable(listOfLoadFolders,
            listOfUpdatedLoadFolders, loadFolders))
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(
            loadMetadataDetails.getLoadStatus)) {
            logInfo(s"Aggregate table creation failed :: aggTable")
            return loadMetadataDetails.getLoadStatus
          }
        }
        loadMetadataDetails.getLoadStatus
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String],
          listOfUpdatedLoadFolders: java.util.List[String],
          loadFolders: Array[String]): String = {
        // TODO: Implement it
        loadMetadataDetails.getLoadStatus
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
      // for table split partition
      val theSplit = split.asInstanceOf[CarbonTableSplitPartition]
      val location = theSplit.serializableHadoopSplit.value.getLocations.asScala
      location
    } else {
      // for node partition
      val theSplit = split.asInstanceOf[CarbonNodePartition]
      val firstOptionLocation: Seq[String] = List(theSplit.serializableHadoopSplit)
      logInfo("Preferred Location for split: " + firstOptionLocation.head)
      val blockMap = new util.LinkedHashMap[String, Integer]()
      val tableBlocks = theSplit.blocksDetails
      tableBlocks.foreach { tableBlock =>
        tableBlock.getLocations.foreach { location =>
          if (!firstOptionLocation.exists(location.equalsIgnoreCase)) {
            val currentCount = blockMap.get(location)
            if (currentCount == null) {
              blockMap.put(location, 1)
            } else {
              blockMap.put(location, currentCount + 1)
            }
          }
        }
      }

      val sortedList = blockMap.entrySet().asScala.toSeq.sortWith((nodeCount1, nodeCount2) => {
        nodeCount1.getValue > nodeCount2.getValue
      }
      )

      val sortedNodesList = sortedList.map(nodeCount => nodeCount.getKey).take(2)
      firstOptionLocation ++ sortedNodesList
    }
  }

}

/**
 * Use this RDD class to load RDD
 *
 * @param sc
 * @param result
 * @param carbonLoadModel
 * @param storePath
 * @param kettleHomePath
 * @param columnar
 * @param loadCount
 * @param tableCreationTime
 * @param schemaLastUpdatedTime
 * @param prev
 * @tparam K
 * @tparam V
 */
class DataFrameLoaderRDD[K, V](
    sc: SparkContext,
    result: DataLoadResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    storePath: String,
    kettleHomePath: String,
    columnar: Boolean,
    loadCount: Integer,
    tableCreationTime: Long,
    schemaLastUpdatedTime: Long,
    prev: DataLoadCoalescedRDD[Row]) extends RDD[(K, V)](prev) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  @DeveloperApi
  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val resultIter = new Iterator[(K, V)] {
      val partitionID = "0"
      val loadMetadataDetails = new LoadMetadataDetails()
      val uniqueLoadStatusId = carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE +
          theSplit.index
      try {
        loadMetadataDetails.setPartitionCount(partitionID)
        carbonLoadModel.setPartitionId(partitionID)
        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        val loader = new SparkPartitionLoader(carbonLoadModel, theSplit.index, storePath,
          kettleHomePath, String.valueOf(loadCount), loadMetadataDetails)
        loader.initialize
        val rddIteratorKey = UUID.randomUUID().toString
        try {
          RddInputUtils.put(rddIteratorKey,
              new PartitionIterator(
                  firstParent[DataLoadPartitionWrap[Row]].iterator(theSplit, context),
                  carbonLoadModel,
                  context))
          carbonLoadModel.setRddIteratorKey(rddIteratorKey)
          loader.run()
        } finally {
          RddInputUtils.remove(rddIteratorKey)
        }
      } catch {
        case e: Exception =>
          logInfo("DataLoad failure")
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
    resultIter
  }

  override protected def getPartitions: Array[Partition] = firstParent[Row].partitions
}

class PartitionIterator(partitionIter: Iterator[DataLoadPartitionWrap[Row]],
    carbonLoadModel: CarbonLoadModel,
    context: TaskContext) extends CarbonIterator[CarbonIterator[Array[String]]] {
  val serializer = SparkEnv.get.closureSerializer.newInstance()
  var serializeBuffer: ByteBuffer = null
  def hasNext: Boolean = partitionIter.hasNext

  def next: CarbonIterator[Array[String]] = {
    val value = partitionIter.next
    // The rdd (which come from Hive Table) don't support to read dataframe concurrently.
    // So here will create different rdd instance for each thread.
    val newInstance = {
      if (serializeBuffer == null) {
        serializeBuffer = serializer.serialize[RDD[Row]](value.rdd)
      }
      serializeBuffer.rewind()
      serializer.deserialize[RDD[Row]](serializeBuffer)
    }
    new RddIterator(newInstance.iterator(value.partition, context),
        carbonLoadModel,
        context)
  }
  override def initialize: Unit = {
    SparkUtil.setTaskContext(context)
  }
}
/**
 * This class wrap Scala's Iterator to Java's Iterator.
 * It also convert all columns to string data to use csv data loading flow.
 *
 * @param rddIter
 * @param carbonLoadModel
 * @param context
 */
class RddIterator(rddIter: Iterator[Row],
                  carbonLoadModel: CarbonLoadModel,
                  context: TaskContext) extends CarbonIterator[Array[String]] {

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

  def next: Array[String] = {
    val row = rddIter.next()
    val columns = new Array[String](row.length)
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

class RddIteratorForUpdate(rddIter: Iterator[Row],
    carbonLoadModel: CarbonLoadModel) extends java.util.Iterator[Array[String]] {
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

  def next: Array[String] = {
    val row = rddIter.next()
    val columns = new Array[String](row.length)
    for (i <- 0 until row.length) {
      // columns(i) = CarbonScalaUtil.getStringForUpdate(row(i), delimiterLevel1, delimiterLevel2)
      columns(i) = CarbonScalaUtil.getString(row.get(i), serializationNullFormat,
        delimiterLevel1, delimiterLevel2, timeStampFormat, dateFormat)
      if (columns(i).length() > CarbonCommonConstants.DEFAULT_COLUMN_LENGTH) {
        sys.error(s" Error processing input: Length of parsed input (${
          CarbonCommonConstants
            .DEFAULT_COLUMN_LENGTH
        }) exceeds the maximum number of characters defined"
        )
      }
    }
    columns
  }

  def remove(): Unit = {
  }
}

object CarbonDataLoadForUpdate {
  def initialize(model: CarbonLoadModel,
      splitIndex: Int): String = {
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
    var storeLocation = ""
    if(carbonUseLocalDir.equalsIgnoreCase("true")) {
      val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
      if (null != storeLocations && storeLocations.nonEmpty) {
        storeLocation = storeLocations(Random.nextInt(storeLocations.length))
      }
      if (storeLocation == null) {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
    }
    else {
      storeLocation = System.getProperty("java.io.tmpdir")
    }
    storeLocation = storeLocation + '/' + System.nanoTime() + '/' + splitIndex
    storeLocation
  }

  def run(model: CarbonLoadModel,
      index: Int,
      hdfsStoreLocation: String,
      kettleHomePath: String,
      loadCount: String,
      loadMetadataDetails: LoadMetadataDetails,
      executorErrors: ExecutionErrors): Unit = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    try {
      var storeLocation = ""
      val carbonUseLocalDir = CarbonProperties.getInstance()
        .getProperty("carbon.use.local.dir", "false")
      if(carbonUseLocalDir.equalsIgnoreCase("true")) {
        val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != storeLocations && storeLocations.nonEmpty) {
          storeLocation = storeLocations(Random.nextInt(storeLocations.length))
        }
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
      }
      else {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
      storeLocation = storeLocation + '/' + System.nanoTime() + '/' + index

      CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation,
        kettleHomePath)
      loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
    } catch {
      case e: DataLoadingException => if (e.getErrorCode ==
                                          DataProcessorConstants.BAD_REC_FOUND) {
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
        LOGGER.info("Bad Record Found")
      } else if (e.getErrorCode == DataProcessorConstants.BAD_REC_FAILURE_ERROR_CODE) {
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)
        executorErrors.failureCauses = FailureCauses.BAD_RECORDS
        executorErrors.errorMsg = e.getMessage
      } else {
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)
        throw e
      }
      case e: Exception =>
        // this will be in case of any other exception where the executor has to rethrow and retry.
        throw e
    } finally {
      // delete temp location data
      try {
        val isCompaction = false
        CarbonLoaderUtil.deleteLocalDataLoadFolderLocation(model, isCompaction)
      } catch {
        case e: Exception =>
          LOGGER.error("Failed to delete local data" + e)
      }
      if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(
        loadMetadataDetails.getLoadStatus)) {
        CarbonTimeStatisticsFactory.getLoadStatisticsInstance.printStatisticsInfo(
          model.getPartitionId)
      }
    }
  }
}

