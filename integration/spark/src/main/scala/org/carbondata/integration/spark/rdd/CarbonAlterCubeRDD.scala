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

package org.carbondata.integration.spark.rdd

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.carbondata.core.carbon.CarbonDef
import org.carbondata.core.util.CarbonUtil
import org.carbondata.integration.spark.RestructureResult
import org.carbondata.integration.spark.util.CarbonQueryUtil
import org.carbondata.processing.restructure.SchemaRestructurer
import org.carbondata.query.datastorage.InMemoryTableStore

class CarbonAlterCubeRDD[K, V](
                                sc: SparkContext,
                                origUnModifiedSchema: CarbonDef.Schema,
                                schema: CarbonDef.Schema,
                                schemaName: String,
                                cubeName: String,
                                hdfsStoreLocation: String,
                                addedDimensions: Seq[CarbonDef.CubeDimension],
                                addedMeasures: Seq[CarbonDef.Measure],
                                validDropDimList: ArrayBuffer[String],
                                validDropMsrList: ArrayBuffer[String],
                                curTime: Long,
                                defaultVals: Map[String, String],
                                currentRestructNumber: Integer,
                                metaDataPath: String,
                                partitioner: Partitioner,
                                result: RestructureResult[K, V])
  extends RDD[(K, V)](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val splits = CarbonQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new CarbonLoadPartition(id, i, splits(i))
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[CarbonLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      logInfo("Input split: " + split.serializableHadoopSplit.value)
      val partitionID = split.serializableHadoopSplit.value.getPartition().getUniqueID()

      val schemaNameWithPartition = schemaName + '_' + partitionID
      val cubeNameWithPartition = cubeName + '_' + partitionID
      val factTableName = schema.cubes(0).fact.toString()

      val schemaRestructureInvoker = new
          SchemaRestructurer(schemaNameWithPartition, cubeNameWithPartition,
            factTableName, hdfsStoreLocation, currentRestructNumber, curTime)
      val status = schemaRestructureInvoker
        .restructureSchema(addedDimensions, addedMeasures, defaultVals, origUnModifiedSchema,
          schema, validDropDimList, validDropMsrList)
      if (status && InMemoryTableStore.getInstance.isLevelCacheEnabled()) {
        val listOfLoadFolders = CarbonQueryUtil
          .getListOfSlices(CarbonUtil.readLoadMetadata(metaDataPath))
        CarbonQueryUtil
          .clearLevelLRUCacheForDroppedColumns(listOfLoadFolders, validDropDimList, schemaName,
            cubeName, partitioner.partitionCount)
      }
      var loadCount: Integer = -1
      if (addedDimensions.length > 0 || addedMeasures.length > 0) {
        loadCount = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(
          getNewLoadFolderLocation(partitionID, factTableName, currentRestructNumber +
            1))
      } else {
        loadCount = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(
          getNewLoadFolderLocation(partitionID, factTableName, currentRestructNumber))
      }
      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var finished = false

      override def hasNext: Boolean = !finished

      override def next(): (K, V) = {
        finished = true
        result.getKey(loadCount, status)
      }

      private def close() {
        try {
          //          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    iter
  }

  private def getNewLoadFolderLocation(partitionID: String, factTableName: String,
                                       restructNumber: Integer): String = {
    hdfsStoreLocation + File.separator + schemaName + '_' + partitionID + File.separator +
      cubeName + '_' + partitionID + File.separator + "RS_" + restructNumber + File.separator +
      factTableName
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

