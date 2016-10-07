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
import java.text.SimpleDateFormat
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.spark.sql.Row

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.common.logging.impl.StandardLogService
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory}
import org.apache.carbondata.processing.constants.DataProcessorConstants
import org.apache.carbondata.processing.csvreaderstep.RddInputUtils
import org.apache.carbondata.processing.etl.DataLoadingException
import org.apache.carbondata.processing.graphgenerator.GraphGenerator
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.load._
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.CarbonQueryUtil

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
                           hdfsStoreLocation: String,
                           kettleHomePath: String,
                           loadCount: Int,
                           loadMetadataDetails: LoadMetadataDetails) extends Logging{

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
  }

  def run(): Unit = {
    try {
      CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation,
        kettleHomePath)
    } catch {
      case e: DataLoadingException => if (e.getErrorCode ==
        DataProcessorConstants.BAD_REC_FOUND) {
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
        logInfo("Bad Record Found")
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
          logError("Failed to delete local data", e)
      }
      if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(
        loadMetadataDetails.getLoadStatus)) {
        if (CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
          .equals(loadMetadataDetails.getLoadStatus)) {
          logInfo("DataLoad complete")
          logInfo("Data Load partially successful with LoadCount:" + loadCount)
        } else {
          logInfo("DataLoad complete")
          logInfo("Data Loaded successfully with LoadCount:" + loadCount)
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
 * @param storeLocation         Tmp store location
 * @param hdfsStoreLocation     The store location in hdfs
 * @param kettleHomePath        The kettle home path
 * @param partitioner           Partitioner which specify how to partition
 * @param columinar             whether it is columinar
 * @param loadCount             Current load count
 * @param tableCreationTime      Time of creating table
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
    var storeLocation: String,
    hdfsStoreLocation: String,
    kettleHomePath: String,
    partitioner: Partitioner,
    columinar: Boolean,
    loadCount: Integer,
    tableCreationTime: Long,
    schemaLastUpdatedTime: Long,
    blocksGroupBy: Array[(String, Array[BlockDetails])],
    isTableSplitPartition: Boolean) extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    isTableSplitPartition match {
      case true =>
        // for table split partition
        var splits = Array[TableSplit]()
        if (carbonLoadModel.isDirectLoad) {
          splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath,
            partitioner.nodeList, partitioner.partitionCount)
        }
        else {
          splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, null, partitioner)
        }

        splits.zipWithIndex.map {s =>
          // filter the same partition unique id, because only one will match, so get 0 element
          val blocksDetails: Array[BlockDetails] = blocksGroupBy.filter(p =>
            p._1 == s._1.getPartition.getUniqueID)(0)._2
          new CarbonTableSplitPartition(id, s._2, s._1, blocksDetails)
        }
      case false =>
        // for node partition
        blocksGroupBy.zipWithIndex.map{b =>
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
      var uniqueLoadStatusId = carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE +
                               theSplit.index
      try {
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)

        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        setModelAndBlocksInfo()
        val loader = new SparkPartitionLoader(model, theSplit.index, hdfsStoreLocation,
          kettleHomePath, loadCount, loadMetadataDetails)
        loader.initialize
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
        if (model.isRetentionRequest) {
          recreateAggregationTableForRetention
        }
        else if (model.isAggLoadRequest) {
          loadMetadataDetails.setLoadStatus(createManualAggregateTable)
        }
        else {
          loader.run
        }
      } catch {
        case e: Exception =>
          logInfo("DataLoad failure")
          LOGGER.error(e)
          throw e
      }

      def setModelAndBlocksInfo(): Unit = {
        isTableSplitPartition match {
          case true =>
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
            CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordPartitionBlockMap(
              partitionID, split.partitionBlocksDetail.length)
          case false =>
            // for node partition
            val split = theSplit.asInstanceOf[CarbonNodePartition]
            logInfo("Input split: " + split.serializableHadoopSplit)
            logInfo("The Block Count in this node :" + split.nodeBlocksDetail.length)
            CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordHostBlockMap(
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
            }
            else {
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
        isTableSplitPartition match {
          case true =>
            carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName + "_" +
            theSplit.asInstanceOf[CarbonTableSplitPartition].serializableHadoopSplit.value
              .getPartition.getUniqueID + "_" + UUID.randomUUID()
          case false =>
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
          }
          else {
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
    isTableSplitPartition match {
      case true =>
        // for table split partition
        val theSplit = split.asInstanceOf[CarbonTableSplitPartition]
        val location = theSplit.serializableHadoopSplit.value.getLocations.asScala
        location
      case false =>
        // for node partition
        val theSplit = split.asInstanceOf[CarbonNodePartition]
        val firstOptionLocation: Seq[String] = List(theSplit.serializableHadoopSplit)
        logInfo("Preferred Location for split : " + firstOptionLocation(0))
        val blockMap = new util.LinkedHashMap[String, Integer]()
        val tableBlocks = theSplit.blocksDetails
        tableBlocks.foreach(tableBlock => tableBlock.getLocations.foreach(
          location => {
            if (!firstOptionLocation.exists(location.equalsIgnoreCase(_))) {
              val currentCount = blockMap.get(location)
              if (currentCount == null) {
                blockMap.put(location, 1)
              } else {
                blockMap.put(location, currentCount + 1)
              }
            }
          }
        )
        )

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
 * @param sc
 * @param result
 * @param carbonLoadModel
 * @param storeLocation
 * @param hdfsStoreLocation
 * @param kettleHomePath
 * @param columinar
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
    var storeLocation: String,
    hdfsStoreLocation: String,
    kettleHomePath: String,
    columinar: Boolean,
    loadCount: Integer,
    tableCreationTime: Long,
    schemaLastUpdatedTime: Long,
    prev: RDD[Row]) extends RDD[(K, V)](prev) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  @DeveloperApi
  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val resultIter = new Iterator[(K, V)] {
      var partitionID = "0"
      val loadMetadataDetails = new LoadMetadataDetails()
      var uniqueLoadStatusId = carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE +
        theSplit.index
      try {
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)
        carbonLoadModel.setPartitionId(partitionID)
        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
        val loader = new SparkPartitionLoader(carbonLoadModel, theSplit.index, hdfsStoreLocation,
          kettleHomePath, loadCount, loadMetadataDetails)
        loader.initialize
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
        val rddIteratorKey = UUID.randomUUID().toString
        try{
          RddInputUtils.put(rddIteratorKey,
            new RddIterator(firstParent[Row].iterator(theSplit, context), carbonLoadModel))
          carbonLoadModel.setRddIteratorKey(rddIteratorKey)
          loader.run
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

/**
 * This class wrap Scala's Iterator to Java's Iterator.
 * It also convert all columns to string data to use csv data loading flow.
 * @param rddIter
 * @param carbonLoadModel
 */
class RddIterator(rddIter: Iterator[Row],
                  carbonLoadModel: CarbonLoadModel) extends java.util.Iterator[Array[String]] {
  val formatString = CarbonProperties.getInstance().getProperty(CarbonCommonConstants
    .CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  val format = new SimpleDateFormat(formatString)
  val delimiterLevel1 = carbonLoadModel.getComplexDelimiterLevel1
  val delimiterLevel2 = carbonLoadModel.getComplexDelimiterLevel2

  def hasNext: Boolean = rddIter.hasNext

  private def getString(value: Any, level: Int = 1): String = {
    value == null match {
      case true => ""
      case false => value match {
        case s: String => s
        case i: java.lang.Integer => i.toString
        case d: java.lang.Double => d.toString
        case t: java.sql.Timestamp => format format t
        case d: java.sql.Date => format format d
        case d: java.math.BigDecimal => d.toPlainString
        case b: java.lang.Boolean => b.toString
        case s: java.lang.Short => s.toString
        case f: java.lang.Float => f.toString
        case bs: Array[Byte] => new String(bs)
        case s: scala.collection.Seq[Any] =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          s.foreach { x =>
            builder.append(getString(x, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - 1)
        case m: scala.collection.Map[Any, Any] =>
          throw new Exception("Unsupported data type: Map")
        case r: org.apache.spark.sql.Row =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          for (i <- 0 until r.length) {
            builder.append(getString(r(i), level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - 1)
        case other => other.toString
      }
    }
  }

  def next: Array[String] = {
    val row = rddIter.next()
    val columns = new Array[String](row.length)
    for (i <- 0 until row.length) {
      columns(i) = getString(row(i))
    }
    columns
  }

  def remove(): Unit = {
  }
}
