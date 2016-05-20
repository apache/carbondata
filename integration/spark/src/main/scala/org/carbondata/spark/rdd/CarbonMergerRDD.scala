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

import java.text.SimpleDateFormat
import java.util.{Date, List}

import scala.collection.JavaConverters._

import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.Partitioner

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.core.util.CarbonUtil
import org.carbondata.spark.MergeResult
import org.carbondata.spark.load._
import org.carbondata.spark.merger.CarbonDataMergerUtil
import org.carbondata.spark.splits.TableSplit
import org.carbondata.spark.util.CarbonQueryUtil

class CarbonMergerRDD[K, V](
    sc: SparkContext,
    result: MergeResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    storeLocation: String,
    hdfsStoreLocation: String,
    partitioner: Partitioner,
    currentRestructNumber: Integer,
    metadataFilePath: String,
    loadsToMerge: List[String],
    mergedLoadName: String,
    kettleHomePath: String,
    cubeCreationTime: Long)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      val split = theSplit.asInstanceOf[CarbonLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)
      val partitionId = split.serializableHadoopSplit.value.getPartition().getUniqueID()
      val model = carbonLoadModel
        .getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID())

      val mergedLoadMetadataDetails = CarbonDataMergerUtil
        .executeMerging(model, storeLocation, hdfsStoreLocation, currentRestructNumber,
          metadataFilePath, loadsToMerge, mergedLoadName)

      model.setLoadMetadataDetails(CarbonUtil
        .readLoadMetadata(metadataFilePath).toList.asJava);

      if (mergedLoadMetadataDetails == true) {
        CarbonLoaderUtil.copyMergedLoadToHDFS(model, currentRestructNumber, mergedLoadName)
        dataloadStatus = checkAndLoadAggregationTable

      }

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !false
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        result.getKey(0, mergedLoadMetadataDetails)
      }


      def checkAndLoadAggregationTable(): String = {
        var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
        val carbonTable = model.getCarbonDataLoadSchema.getCarbonTable
        val aggTables = carbonTable.getAggregateTablesName
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.asScala.toSeq.toArray
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + mergedLoadName
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders);
          var listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
          listOfAllLoadFolders = CarbonLoaderUtil
            .addNewSliceNameToList(newSlice, listOfAllLoadFolders);
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
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
          dataloadStatus = iterateOverAggTables(aggTables.asScala.toArray,
            copyListOfLoadFolders.asJava,
            copyListOfUpdatedLoadFolders.asJava,
            loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            // remove the current slice from memory not the cube
            CarbonLoaderUtil
              .removeSliceFromMemory(model.getDatabaseName, model.getTableName, newSlice)
            logInfo(s"Aggregate table creation failed")
          } else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        dataloadStatus
      }


      def loadCubeSlices(listOfLoadFolders: java.util.List[String],
          deatails: Array[LoadMetadataDetails]) = {

        // TODO: Implement it
      }

      def iterateOverAggTables(aggTables: Array[String],
          listOfLoadFolders: java.util.List[String],
          listOfUpdatedLoadFolders: java.util.List[String],
          loadFolders: Array[String]): String = {
        model.setAggLoadRequest(true)
        aggTables.foreach { aggTableName =>
          model.setAggTableName(aggTableName)
          dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders,
            loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            logInfo(s"Aggregate table creation failed :: $aggTableName")
            return CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          }
        }
        CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String],
          listOfUpdatedLoadFolders: java.util.List[String],
          loadFolders: Array[String]): String = {
        loadFolders.foreach { loadFolder =>
          val restructNumber = CarbonUtil.getRestructureNumber(loadFolder, model.getTableName)
          try { {
            if (CarbonLoaderUtil
              .isSliceValid(loadFolder, listOfLoadFolders, listOfUpdatedLoadFolders,
                model.getTableName)) {
              model.setFactStoreLocation(loadFolder)
              CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath,
                restructNumber)
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
            } else {
              CarbonLoaderUtil
                .createEmptyLoadFolder(model, loadFolder, hdfsStoreLocation, restructNumber)
            }
          }
          } catch {
            case e: Exception => dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } finally {
            if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val loadName = loadFolder
                .substring(loadFolder.indexOf(CarbonCommonConstants.LOAD_FOLDER))
              CarbonLoaderUtil.copyCurrentLoadToHDFS(model, loadName, listOfUpdatedLoadFolders)
            } else {
              logInfo(s"Load creation failed :: $loadFolder")
              return CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
            }
          }
        }
        return CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      }


    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations.asScala
    logInfo("Host Name : " + s(0) + s.length)
    s
  }

  override def getPartitions: Array[Partition] = {
    val splits = CarbonQueryUtil
      .getTableSplits(carbonLoadModel.getDatabaseName(), carbonLoadModel.getTableName(), null,
        partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new CarbonLoadPartition(id, i, splits(i))
    }
    result
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }
}

class CarbonLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}
