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
package org.apache.spark.util

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.executor.InputMetrics
import org.apache.spark.SerializableWritable
import org.apache.spark.TaskContext

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit

/**
 * It gives statistics of number of bytes and record read
 */
class CarbonInputMetrics extends InputMetricsStats {
  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  var inputMetrics: InputMetrics = _
  var bytesReadCallback: Option[() => Long] = _
  var existingBytesRead: Long = _
  def incrementRecordRead(recordRead: Long) {
    inputMetrics.incRecordsRead(recordRead)
    if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
      updateBytesRead()
    }
  }

  def initBytesReadCallback(context: TaskContext,
                            carbonMultiBlockSplit: CarbonMultiBlockSplit) {
    inputMetrics = context.taskMetrics().inputMetrics
    existingBytesRead = inputMetrics.bytesRead
    bytesReadCallback = carbonMultiBlockSplit match {
      case _: CarbonMultiBlockSplit =>
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
      case _ => None
    }
  }

  def updateBytesRead(): Unit = {
    bytesReadCallback.foreach { getBytesRead =>
      inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
    }
  }

  def updateBytesRead(serializableWritable: SerializableWritable[CarbonMultiBlockSplit]) {
    if (bytesReadCallback.isDefined) {
      updateBytesRead()
    } else if (serializableWritable.value.isInstanceOf[CarbonMultiBlockSplit]) {
      // If we can't get the bytes read from the FS stats, fall back to the split size,
      // which may be inaccurate.
      try {
        inputMetrics.incBytesRead(serializableWritable.value.getLength)
      } catch {
        case e: java.io.IOException =>
          LOGGER.warn("Unable to get input size to set InputMetrics for task:" + e.getMessage)
      }
    }
  }
}
