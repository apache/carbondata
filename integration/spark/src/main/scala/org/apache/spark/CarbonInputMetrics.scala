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
package org.apache.spark

import java.lang.Long

import org.apache.spark.executor.InputMetrics

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.TaskMetricsMap
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit
import org.apache.carbondata.spark.InitInputMetrics


/**
 * It gives statistics of number of bytes and record read
 */
class CarbonInputMetrics extends InitInputMetrics{
  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    var inputMetrics: InputMetrics = _
    // bytes read before compute by other map rdds in lineage
    var existingBytesRead: Long = _
    var recordCount: Long = _
    var inputMetricsInterval: Long = _
    var carbonMultiBlockSplit: CarbonMultiBlockSplit = _

  def initBytesReadCallback(context: TaskContext,
      carbonMultiBlockSplit: CarbonMultiBlockSplit, inputMetricsInterval: Long) {
    inputMetrics = context.taskMetrics().inputMetrics
    existingBytesRead = inputMetrics.bytesRead
    recordCount = 0L
    this.inputMetricsInterval = inputMetricsInterval
    this.carbonMultiBlockSplit = carbonMultiBlockSplit
  }

  def incrementRecordRead(recordRead: Long) {
    val value: scala.Long = recordRead
    recordCount = recordCount + value
    if (recordCount > inputMetricsInterval) {
      inputMetrics.synchronized {
        inputMetrics.incRecordsRead(recordCount)
        updateBytesRead()
      }
      recordCount = 0L
    }
  }

  def updateBytesRead(): Unit = {
    inputMetrics
      .setBytesRead(existingBytesRead
                    + TaskMetricsMap.getInstance().getReadBytesSum(Thread.currentThread().getId))
  }

  def updateAndClose() {
    if (recordCount > 0L) {
      inputMetrics.synchronized {
        inputMetrics.incRecordsRead(recordCount)
      }
      recordCount = 0L
    }
    // if metrics supported file system ex: hdfs
    if (!TaskMetricsMap.getInstance().isCallbackEmpty(Thread.currentThread().getId)) {
      updateBytesRead()
      // after update clear parent thread entry from map.
      TaskMetricsMap.getInstance().removeEntry(Thread.currentThread().getId)
    } else if (carbonMultiBlockSplit.isInstanceOf[CarbonMultiBlockSplit]) {
      // If we can't get the bytes read from the FS stats, fall back to the split size,
      // which may be inaccurate.
      try {
        inputMetrics.incBytesRead(carbonMultiBlockSplit.getLength)
      } catch {
        case e: java.io.IOException =>
          LOGGER.warn("Unable to get input size to set InputMetrics for task:" + e.getMessage)
      }
    }
  }

  override def updateByValue(value: Object): Unit = {

  }
}
