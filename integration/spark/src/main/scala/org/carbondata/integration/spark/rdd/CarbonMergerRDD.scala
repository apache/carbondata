/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.rdd

import java.text.SimpleDateFormat
import java.util.{Date, List}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, TaskContext}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark.MergeResult
import org.carbondata.integration.spark.load.{CarbonLoadModel, CarbonLoaderUtil}
import org.carbondata.integration.spark.merger.CarbonDataMergerUtil
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.CarbonQueryUtil
import org.carbondata.query.datastorage.InMemoryTableStore

import scala.collection.JavaConversions._

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

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      val split = theSplit.asInstanceOf[CarbonLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)
      val partitionId = split.serializableHadoopSplit.value.getPartition().getUniqueID()
      val model = carbonLoadModel.getCopyWithPartition(split.serializableHadoopSplit.value.getPartition().getUniqueID(),null)

      val mergedLoadMetadataDetails = CarbonDataMergerUtil.executeMerging(model, storeLocation, hdfsStoreLocation, currentRestructNumber, metadataFilePath, loadsToMerge, mergedLoadName)

      model.setLoadMetadataDetails(CarbonUtil
        .readLoadMetadata(metadataFilePath).toList);

      if (mergedLoadMetadataDetails == true) {
        CarbonLoaderUtil.copyMergedLoadToHDFS(model, currentRestructNumber, mergedLoadName)
        dataloadStatus = checkAndLoadAggregationTable

      }

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
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
        /*  val row = new CarbonKey(null)
          val value = new CarbonValue(null)*/
        result.getKey(0, mergedLoadMetadataDetails)
      }

      private def close() {
        try {
          //          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }

      def checkAndLoadAggregationTable(): String = {
        var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
        val schema = model.getSchema
        val aggTables = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
        if (null != aggTables && !aggTables.isEmpty) {
          val details = model.getLoadMetadataDetails.toSeq.toArray
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + mergedLoadName
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          listOfLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfLoadFolders);
          var listOfAllLoadFolders = CarbonQueryUtil.getListOfSlices(details)
          listOfAllLoadFolders = CarbonLoaderUtil.addNewSliceNameToList(newSlice, listOfAllLoadFolders);
          val listOfUpdatedLoadFolders = CarbonLoaderUtil.getListOfUpdatedSlices(details)
          val copyListOfLoadFolders = listOfLoadFolders.toList
          val copyListOfUpdatedLoadFolders = listOfUpdatedLoadFolders.toList
          loadCubeSlices(listOfAllLoadFolders, details)
          var loadFolders = Array[String]()
          val loadFolder = CarbonLoaderUtil.getAggLoadFolderLocation(newSlice, model.getSchemaName, model.getCubeName, model.getTableName, hdfsStoreLocation, currentRestructNumber)
          if (null != loadFolder) {
            loadFolders :+= loadFolder
          }
          dataloadStatus = iterateOverAggTables(aggTables, copyListOfLoadFolders, copyListOfUpdatedLoadFolders, loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            // remove the current slice from memory not the cube
            CarbonLoaderUtil.removeSliceFromMemory(model.getSchemaName, model.getCubeName, newSlice)
            logInfo(s"Aggregate table creation failed")
          } else {
            logInfo("Aggregate tables creation successfull")
          }
        }
        dataloadStatus
      }


      def loadCubeSlices(listOfLoadFolders: java.util.List[String], deatails: Array[LoadMetadataDetails]) = {
        CarbonProperties.getInstance().addProperty("carbon.cache.used", "false");
        CarbonQueryUtil.createDataSource(currentRestructNumber, model.getSchema, null, partitionId, listOfLoadFolders, model.getTableName, hdfsStoreLocation, cubeCreationTime, deatails)
      }

      def iterateOverAggTables(aggTables: Array[CarbonDef.AggTable], listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
        model.setAggLoadRequest(true)
        aggTables.foreach { aggTable =>
          val aggTableName = CarbonLoaderUtil.getAggregateTableName(aggTable)
          model.setAggTableName(aggTableName)
          dataloadStatus = loadAggregationTable(listOfLoadFolders, listOfUpdatedLoadFolders, loadFolders)
          if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
            logInfo(s"Aggregate table creation failed :: $aggTableName")
            return CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          }
        }
        CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      }

      def loadAggregationTable(listOfLoadFolders: java.util.List[String], listOfUpdatedLoadFolders: java.util.List[String], loadFolders: Array[String]): String = {
        loadFolders.foreach { loadFolder =>
          val restructNumber = CarbonUtil.getRestructureNumber(loadFolder, model.getTableName)
          try {
            if (CarbonLoaderUtil.isSliceValid(loadFolder, listOfLoadFolders, listOfUpdatedLoadFolders, model.getTableName)) {
              model.setFactStoreLocation(loadFolder)
              CarbonLoaderUtil.executeGraph(model, storeLocation, hdfsStoreLocation, kettleHomePath, restructNumber)
              dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
            } else {
              CarbonLoaderUtil.createEmptyLoadFolder(model, loadFolder, hdfsStoreLocation, restructNumber)
            }
          } catch {
            case e: Exception => dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } finally {
            if (!CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equals(dataloadStatus)) {
              val loadName = loadFolder.substring(loadFolder.indexOf(CarbonCommonConstants.LOAD_FOLDER))
              CarbonLoaderUtil.copyCurrentLoadToHDFS(model, restructNumber, loadName, listOfUpdatedLoadFolders, restructNumber)
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
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }

  override def getPartitions: Array[Partition] = {
    val splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getSchemaName(), carbonLoadModel.getCubeName(), null, partitioner)
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