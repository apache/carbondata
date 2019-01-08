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

package org.apache.spark.sql.carbondata.execution.datasources.tasklisteners

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.util.TaskCompletionListener

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.memory.UnsafeMemoryManager
import org.apache.carbondata.core.util.{DataTypeUtil, ThreadLocalTaskInfo}
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable

/**
 * Query completion listener
 */
trait CarbonQueryTaskCompletionListener extends TaskCompletionListener

/**
 * Load completion listener
 */
trait CarbonLoadTaskCompletionListener extends TaskCompletionListener

case class CarbonQueryTaskCompletionListenerImpl(iter: RecordReaderIterator[InternalRow],
    freeMemory: Boolean = false) extends CarbonQueryTaskCompletionListener {
  override def onTaskCompletion(context: TaskContext): Unit = {
    if (iter != null) {
      try {
        iter.close()
      } catch {
        case e: Exception =>
          LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(e)
      }
    }
    if (freeMemory) {
      UnsafeMemoryManager.INSTANCE
        .freeMemoryAll(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)
      ThreadLocalTaskInfo.clearCarbonTaskInfo()
    }
    DataTypeUtil.clearFormatter()
  }
}

case class CarbonLoadTaskCompletionListenerImpl(recordWriter: RecordWriter[NullWritable,
  ObjectArrayWritable],
    taskAttemptContext: TaskAttemptContext) extends CarbonLoadTaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    try {
      recordWriter.close(taskAttemptContext)
    } finally {
      UnsafeMemoryManager.INSTANCE
        .freeMemoryAll(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)
      ThreadLocalTaskInfo.clearCarbonTaskInfo()
      DataTypeUtil.clearFormatter()
    }
  }
}
