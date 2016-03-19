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

package com.huawei.datasight.spark.rdd

import scala.collection.JavaConversions._
import org.apache.spark.Logging
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cubemodel.Partitioner
import com.huawei.datasight.molap.load.MolapLoadModel
import com.huawei.datasight.molap.spark.util.MolapQueryUtil
import com.huawei.datasight.spark.KeyVal
import com.huawei.unibi.molap.engine.scanner.impl.MolapKey
import com.huawei.unibi.molap.engine.scanner.impl.MolapValue
import com.huawei.datasight.spark.DeletedLoadResultImpl
import com.huawei.datasight.spark.DeletedLoadResult
import com.huawei.datasight.molap.load.DeletedLoadMetadata
import com.huawei.unibi.molap.constants.MolapCommonConstants
import com.huawei.unibi.molap.dataprocessor.dataretention.DataRetentionHandler
import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import com.huawei.datasight.molap.core.load.LoadMetadataDetails

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
  extends RDD[(K, V)](sc, Nil)
    with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")
  
  /**
    * Create the split for each region server.
    */
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
      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback(() => close())
      var finished = false

      override def hasNext: Boolean = {
        mapOfRetentionValues.hasNext

      }


      override def next(): (K, V) = {
        val (loadid, status) = mapOfRetentionValues.next
        println("loadid :" + loadid + "status :" + status)
        result.getKey(loadid, status)
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

  /**
    * Get the preferred locations where to lauch this task.
    */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[MolapLoadPartition]
    val s = theSplit.serializableHadoopSplit.value.getLocations //.filter(_ != "localhost")
    logInfo("Host Name : " + s(0) + s.length)
    s
  }
}

