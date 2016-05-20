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


package org.carbondata.spark.rdd

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.common.logging.impl.StandardLogService
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.carbondata.core.util.CarbonProperties
import org.carbondata.processing.constants.DataProcessorConstants
import org.carbondata.processing.etl.DataLoadingException
import org.carbondata.processing.graphgenerator.GraphGenerator
import org.carbondata.spark.Result
import org.carbondata.spark.load._
import org.carbondata.spark.splits.TableSplit
import org.carbondata.spark.util.CarbonQueryUtil

/**
 * This partition class use to split by TableSplit
 *
 * @param rddId
 * @param idx
 * @param tableSplit
 * @param blocksDetails
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
 * @param rddId
 * @param idx
 * @param host
 * @param blocksDetails
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
 * Use this RDD class to load data
 *
 * @param sc                    The SparkContext to associate the RDD with.
 * @param result                Output result
 * @param carbonLoadModel       Carbon load model which contain the load info
 * @param storeLocation         Tmp store location
 * @param hdfsStoreLocation     The store location in hdfs
 * @param kettleHomePath        The kettle home path
 * @param partitioner           Partitioner which specify how to partition
 * @param columinar             whether it is columinar
 * @param currentRestructNumber current restruct number
 * @param loadCount             Current load count
 * @param cubeCreationTime      Time of creating cube
 * @param schemaLastUpdatedTime Time of last schema update
 * @param blocksGroupBy         Blocks Array which is group by partition or host
 * @param isTableSplitPartition Whether using table split partition
 * @tparam K Class of the key associated with the Result.
 * @tparam V Class of the value associated with the Result.
 */
class CarbonDataLoadRDD[K, V](
    sc: SparkContext,
    result: Result[K, V],
    carbonLoadModel: CarbonLoadModel,
    var storeLocation: String,
    hdfsStoreLocation: String,
    kettleHomePath: String,
    partitioner: Partitioner,
    columinar: Boolean,
    currentRestructNumber: Integer,
    loadCount: Integer,
    cubeCreationTime: Long,
    schemaLastUpdatedTime: Long,
    blocksGroupBy: Array[(String, Array[BlockDetails])],
    isTableSplitPartition: Boolean)
  extends RDD[(K, V)](sc, Nil)
    with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    isTableSplitPartition match {
      case true =>
        // for table split partition
        var splits = Array[TableSplit]()
        if (carbonLoadModel.isDirectLoad()) {
          splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath(),
            partitioner.nodeList, partitioner.partitionCount)
        }
        else {
          splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName(),
            carbonLoadModel.getTableName(), null, partitioner)
        }

        val result = new Array[Partition](splits.length)
        for (i <- 0 until result.length) {
          // filter the same partition unique id, because only one will match, so get 0 element
          val blocksDetails: Array[BlockDetails] = blocksGroupBy.filter(p =>
            p._1 == splits(i).getPartition.getUniqueID)(0)._2
          result(i) = new CarbonTableSplitPartition(id, i, splits(i), blocksDetails)
        }
        result
      case false =>
        // for node partition
        val result = new Array[Partition](blocksGroupBy.length)
        for (i <- 0 until result.length) {
          result(i) = new CarbonNodePartition(id, i, blocksGroupBy(i)._1, blocksGroupBy(i)._2)
        }
        result
    }
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      var partitionID = "0"
      var model: CarbonLoadModel = _

      try { {
        val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
        if (null == carbonPropertiesFilePath) {
          System.setProperty("carbon.properties.filepath",
            System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties");
        }
        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        setModelAndBlocksInfo()
        CarbonProperties.getInstance().addProperty("carbon.is.columnar.storage", "true")
        CarbonProperties.getInstance().addProperty("carbon.dimension.split.value.in.columnar", "1")
        CarbonProperties.getInstance().addProperty("carbon.is.fullyfilled.bits", "true")
        CarbonProperties.getInstance().addProperty("is.int.based.indexer", "true")
        CarbonProperties.getInstance().addProperty("aggregate.columnar.keyblock", "true")
        CarbonProperties.getInstance().addProperty("high.cardinality.value", "100000")
        CarbonProperties.getInstance().addProperty("is.compressed.keyblock", "false")
        CarbonProperties.getInstance().addProperty("carbon.leaf.node.size", "120000")
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
          storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
        }

        dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS

        if (model.isRetentionRequest()) {
          recreateAggregationTableForRetention
        }
        else if (model.isAggLoadRequest()) {
          dataloadStatus = createManualAggregateTable
        }
        else {
          try { {
            CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath,
              currentRestructNumber)
          };
          } catch {
            case e: DataLoadingException => if (e.getErrorCode ==
                                                DataProcessorConstants.BAD_REC_FOUND) {
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              logInfo("Bad Record Found")
            } else {
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
              LOGGER.error(e)
            }
            case e: Exception =>
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
              LOGGER.error(e)
          } finally {
            if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val newSlice = CarbonCommonConstants.LOAD_FOLDER + loadCount
              var isCopyFailed = false
              try { {
                CarbonLoaderUtil.copyCurrentLoadToHDFS(model, newSlice, null)
              };
              } catch {
                case e: Exception =>
                  isCopyFailed = true
                  dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
                  LOGGER.error(e)
              }
              if (!isCopyFailed) {
                dataloadStatus = checkAndLoadAggregationTable
              }
              if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
                logInfo("DataLoad failure")
              } else {
                logInfo("DataLoad complete")
                logInfo("Data Loaded successfully with LoadCount:" + loadCount)
              }
            } else {
              logInfo("DataLoad failure")
            }
          }
        }
      }

      } catch {
        case e: Exception =>
          dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          logInfo("DataLoad failure")
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
            if (carbonLoadModel.isDirectLoad()) {
              model = carbonLoadModel.getCopyWithPartition(
                split.serializableHadoopSplit.value.getPartition().getUniqueID(),
                split.serializableHadoopSplit.value.getPartition().getFilesPath,
                carbonLoadModel.getCsvHeader(), carbonLoadModel.getCsvDelimiter())
            } else {
              model = carbonLoadModel.getCopyWithPartition(
                split.serializableHadoopSplit.value.getPartition().getUniqueID())
            }
            partitionID = split.serializableHadoopSplit.value.getPartition().getUniqueID()
            // get this partition data blocks and put it to global static map
            GraphGenerator.blockInfo.put(blocksID, split.partitionBlocksDetail)
            StandardLogService.setThreadName(partitionID, null)
          case false =>
            // for node partition
            val split = theSplit.asInstanceOf[CarbonNodePartition]
            logInfo("Input split: " + split.serializableHadoopSplit)
            logInfo("The Block Count in this node :" + split.nodeBlocksDetail.length)
            val blocksID = gernerateBlocksID
            carbonLoadModel.setBlocksID(blocksID)
            carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
            // set this node blocks info to global static map
            GraphGenerator.blockInfo.put(blocksID, split.nodeBlocksDetail)
            if (carbonLoadModel.isDirectLoad()) {
              val filelist: java.util.List[String] = new java.util.ArrayList[String](
                CarbonCommonConstants.CONSTANT_SIZE_TEN)
              CarbonQueryUtil.getAllFiles(carbonLoadModel.getFactFilePath, filelist,
                FileFactory.getFileType(carbonLoadModel.getFactFilePath))
              model = carbonLoadModel.getCopyWithPartition(partitionID, filelist,
                carbonLoadModel.getCsvHeader(), carbonLoadModel.getCsvDelimiter())
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
      def gernerateBlocksID(): String = {
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

      def checkAndLoadAggregationTable(): String = {
        val schema = model.getCarbonDataLoadSchema
        val aggTables = schema.getCarbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.asScala.toSeq.toArray
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + loadCount
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders)
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          var listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
          listOfAllLoadFolders = CarbonLoaderUtil
            .addNewSliceNameToList(newSlice, listOfAllLoadFolders)
          val copyListOfLoadFolders = listOfLoadFolders.asScala.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.asScala.toList
          loadCubeSlices(listOfAllLoadFolders, details)
          var loadFolders = Array[String]()
          val loadFolder = CarbonLoaderUtil
            .getAggLoadFolderLocation(newSlice, model.getDatabaseName, model.getTableName,
              model.getTableName, hdfsStoreLocation, currentRestructNumber)
          if (null != loadFolder) {
            loadFolders :+= loadFolder
          }
          dataloadStatus = iterateOverAggTables(aggTables, copyListOfLoadFolders.asJava,
            copyListOfUpdatedLoadFolders.asJava, loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            // remove the current slice from memory not the cube
            CarbonLoaderUtil
              .removeSliceFromMemory(model.getDatabaseName, model.getTableName, newSlice)
            logInfo(s"Aggregate table creation failed")
          }
          else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        return dataloadStatus
      }

      def loadCubeSlices(listOfAllLoadFolders: java.util.List[String],
          loadMetadataDetails: Array[LoadMetadataDetails]) = {
        CarbonProperties.getInstance().addProperty("carbon.cache.used", "false");
        // TODO: Implement it
      }

      def createManualAggregateTable(): String = {
        val details = model.getLoadMetadataDetails.asScala.toSeq.toArray
        val listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
        val listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
        val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
        loadCubeSlices(listOfAllLoadFolders, details)
        var loadFolders = Array[String]()
        var restructFolders = Array[String]()
        for (number <- 0 to currentRestructNumber) {
          restructFolders = CarbonLoaderUtil
            .getStorelocs(model.getDatabaseName, model.getTableName, model.getTableName,
              hdfsStoreLocation, number)
          loadFolders = loadFolders ++ restructFolders
        }
        val aggTable = model.getAggTableName
        dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders,
          loadFolders)
        if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
          logInfo(s"Aggregate table creation failed :: $aggTable")
        } else {
          logInfo(s"Aggregate table creation successfull :: $aggTable")
        }
        dataloadStatus
      }

      def recreateAggregationTableForRetention() = {
        val schema = model.getCarbonDataLoadSchema
        val aggTables = schema.getCarbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.asScala.toSeq.toArray
          val listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          val listOfAllLoadFolder = CarbonQueryUtil.getListOfSlices(details)
          loadCubeSlices(listOfAllLoadFolder, details)
          var loadFolders = Array[String]()
          listOfUpdatedLoadFolders.asScala.foreach { sliceNum =>
            val newSlice = CarbonCommonConstants.LOAD_FOLDER + sliceNum
            val loadFolder = CarbonLoaderUtil
              .getAggLoadFolderLocation(newSlice, model.getDatabaseName, model.getTableName,
                model.getTableName, hdfsStoreLocation, currentRestructNumber)
            if (null != loadFolder) {
              loadFolders :+= loadFolder
            }
          }
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
          dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders,
            loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            logInfo(s"Aggregate table creation failed :: aggTable")
            return dataloadStatus
          }
        }
        return dataloadStatus
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String],
          listOfUpdatedLoadFolders: java.util.List[String],
          loadFolders: Array[String]): String = {
        // TODO: Implement it
        return dataloadStatus
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

      override def next(): (K, V) = {
        val loadMetadataDetails = new LoadMetadataDetails()
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(dataloadStatus.toString())
        result.getKey(loadCount, loadMetadataDetails)
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
        val location: Seq[String] = List(theSplit.serializableHadoopSplit)
        logInfo("Prefered Location for split : " + location(0))
        location
    }
  }
}

