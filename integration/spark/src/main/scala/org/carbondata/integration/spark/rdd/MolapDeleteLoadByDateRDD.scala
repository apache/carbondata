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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.carbondata.core.constants.MolapCommonConstants
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.integration.spark.DeletedLoadResult
import org.carbondata.integration.spark.load.DeletedLoadMetadata
import org.carbondata.integration.spark.util.MolapQueryUtil
import org.carbondata.processing.dataprocessor.dataretention.DataRetentionHandler

import scala.collection.JavaConversions._

class MolapDeleteLoadByDateRDD[K, V](
                                      sc: SparkContext,
                                      result: DeletedLoadResult[K, V],
                                      schemaName: String,
                                      cubeName: String,
                                      dateField: String,
                                      dateFieldActualName: String,
                                      dateValue: String,
                                      partitioner: Partitioner,
                                      factTableName: String,
                                      dimTableName: String,
                                      hdfsStoreLocation: String,
                                      loadMetadataDetails: List[LoadMetadataDetails],
                                      currentRestructFolder: Integer)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  override def getPartitions: Array[Partition] = {
    println(partitioner.nodeList)
    val splits = MolapQueryUtil.getTableSplits(schemaName, cubeName, null, partitioner)
    val result = new Array[Partition](splits.length)
    for (i <- 0 until result.length) {
      result(i) = new MolapLoadPartition(id, i, splits(i))
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new Iterator[(K, V)] {
      val deletedMetaData = new DeletedLoadMetadata()
      val split = theSplit.asInstanceOf[MolapLoadPartition]
      logInfo("Input split: " + split.serializableHadoopSplit.value)

      logInfo("Input split: " + split.serializableHadoopSplit.value)
      val partitionID = split.serializableHadoopSplit.value.getPartition().getUniqueID()

      //TODO call MOLAP delete API
      println("Applying data retention as per date value " + dateValue)
      var dateFormat = ""
      try {
        val dateValueAsDate = DateTimeUtils.stringToTime(dateValue)
        dateFormat = MolapCommonConstants.MOLAP_TIMESTAMP_DEFAULT_FORMAT
      } catch {
        case e: Exception => logInfo("Unable to parse with default time format " + dateValue)
      }
      val dataRetentionInvoker = new DataRetentionHandler(schemaName + '_' + partitionID, cubeName + '_' + partitionID, factTableName, dimTableName, hdfsStoreLocation, dateField, dateFieldActualName, dateValue, dateFormat, currentRestructFolder, loadMetadataDetails)
      val mapOfRetentionValues = dataRetentionInvoker.updateFactFileBasedOnDataRetentionPolicy().toIterator
      var finished = false

      override def hasNext: Boolean = {
        mapOfRetentionValues.hasNext
      }

      override def next(): (K, V) = {
        val (loadid, status) = mapOfRetentionValues.next
        println("loadid :" + loadid + "status :" + status)
        result.getKey(loadid, status)
      }
    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

