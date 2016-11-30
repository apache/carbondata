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
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.common.logging.impl.StandardLogService
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory
import org.apache.carbondata.hadoop.csv.CSVInputFormat
import org.apache.carbondata.hadoop.csv.recorditerator.RecordReaderIterator
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.DataLoadExecutor
import org.apache.carbondata.processing.newflow.exception.BadRecordFoundException
import org.apache.carbondata.spark.DataLoadResult
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.CarbonQueryUtil
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskType}

import scala.util.control.NonFatal

class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

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
      var uniqueLoadStatusId =
        carbonLoadModel.getTableName + CarbonCommonConstants.UNDERSCORE + theSplit.index
      try {
        loadMetadataDetails.setPartitionCount(partitionID)
        loadMetadataDetails.setLoadStatus(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)

        carbonLoadModel.setSegmentId(String.valueOf(loadCount))
        val recordReaders = getInputIterators
        val loader = new SparkPartitionLoader(model,
          theSplit.index,
          null,
          null,
          loadCount,
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

      def getInputIterators: Array[util.Iterator[Array[AnyRef]]] = {
        val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, theSplit.index, 0)
        val configuration: Configuration = confBroadcast.value.value
        configureCSVInputFormat(configuration)
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
          readers.zipWithIndex.foreach { case (reader, index) =>
            reader.initialize(split.partitionBlocksDetail(index), hadoopAttemptContext)
          }
          readers.map(new RecordReaderIterator(_))
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
          readers.zipWithIndex.foreach { case (reader, index) =>
            reader.initialize(split.nodeBlocksDetail(index), hadoopAttemptContext)
          }
          readers.map(new RecordReaderIterator(_))
        }
      }

      def configureCSVInputFormat(configuration: Configuration): Unit = {
        CSVInputFormat.setCommentCharacter(carbonLoadModel.getCommentChar, configuration)
        CSVInputFormat.setCSVDelimiter(carbonLoadModel.getCsvDelimiter, configuration)
        CSVInputFormat.setEscapeCharacter(carbonLoadModel.getEscapeChar, configuration)
        CSVInputFormat.setHeaderExtractionEnabled(
          carbonLoadModel.getCsvHeader == null || carbonLoadModel.getCsvHeader.isEmpty,
          configuration)
        CSVInputFormat.setQuoteCharacter(carbonLoadModel.getQuoteChar, configuration)
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

        val sortedList = blockMap.entrySet().asScala.toSeq.sortWith {(nodeCount1, nodeCount2) =>
          nodeCount1.getValue > nodeCount2.getValue
        }

        val sortedNodesList = sortedList.map(nodeCount => nodeCount.getKey).take(2)
        firstOptionLocation ++ sortedNodesList
    }
  }
}
