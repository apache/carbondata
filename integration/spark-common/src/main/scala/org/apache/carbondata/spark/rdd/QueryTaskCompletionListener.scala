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

import scala.collection.JavaConverters._

import org.apache.hadoop.mapreduce.RecordReader
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.carbondata.execution.datasources.tasklisteners.CarbonQueryTaskCompletionListener
import org.apache.spark.sql.profiler.{Profiler, QueryTaskEnd}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.memory.UnsafeMemoryManager
import org.apache.carbondata.core.stats.{QueryStatistic, QueryStatisticsConstants, QueryStatisticsRecorder}
import org.apache.carbondata.core.util.{DataTypeUtil, TaskMetricsMap, ThreadLocalTaskInfo}
import org.apache.carbondata.spark.InitInputMetrics

class QueryTaskCompletionListener(freeMemory: Boolean,
    var reader: RecordReader[Void, Object],
    inputMetricsStats: InitInputMetrics, executionId: String, taskId: Int, queryStartTime: Long,
    queryStatisticsRecorder: QueryStatisticsRecorder, split: Partition, queryId: String)
  extends CarbonQueryTaskCompletionListener {
  override def onTaskCompletion(context: TaskContext): Unit = {
    if (reader != null) {
      try {
        reader.close()
      } catch {
        case e: Exception =>
          LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(e)
      }
      reader = null
    }
    TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId)
    inputMetricsStats.updateAndClose()
    logStatistics(executionId, taskId, queryStartTime, queryStatisticsRecorder, split)
    if (freeMemory) {
      UnsafeMemoryManager.INSTANCE
        .freeMemoryAll(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)
      ThreadLocalTaskInfo.clearCarbonTaskInfo()
      DataTypeUtil.clearFormatter()
    }
  }

  def logStatistics(
      executionId: String,
      taskId: Long,
      queryStartTime: Long,
      recorder: QueryStatisticsRecorder,
      split: Partition
  ): Unit = {
    if (null != recorder) {
      val queryStatistic = new QueryStatistic()
      queryStatistic.addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
        System.currentTimeMillis - queryStartTime)
      recorder.recordStatistics(queryStatistic)
      // print executor query statistics for each task_id
      val statistics = recorder.statisticsForTask(taskId, queryStartTime)
      if (statistics != null && executionId != null) {
        Profiler.invokeIfEnable {
          val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
          inputSplit.calculateLength()
          val size = inputSplit.getLength
          val files = inputSplit.getAllSplits.asScala.map { s =>
            s.getSegmentId + "/" + s.getPath.getName
          }.toArray[String]
          Profiler.send(
            QueryTaskEnd(
              executionId.toLong,
              queryId,
              statistics.getValues,
              size,
              files
            )
          )
        }
      }
      recorder.logStatisticsForTask(statistics)
    }
  }
}

