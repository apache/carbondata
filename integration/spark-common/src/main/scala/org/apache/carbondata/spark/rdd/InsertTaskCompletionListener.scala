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

import org.apache.spark.TaskContext
import org.apache.spark.sql.carbondata.execution.datasources.tasklisteners.CarbonLoadTaskCompletionListener
import org.apache.spark.sql.execution.command.ExecutionErrors

import org.apache.carbondata.core.util.{DataTypeUtil, ThreadLocalTaskInfo}
import org.apache.carbondata.processing.loading.{DataLoadExecutor, FailureCauses}
import org.apache.carbondata.spark.util.CommonUtil

class InsertTaskCompletionListener(dataLoadExecutor: DataLoadExecutor,
    executorErrors: ExecutionErrors)
  extends CarbonLoadTaskCompletionListener {
  override def onTaskCompletion(context: TaskContext): Unit = {
    try {
      dataLoadExecutor.close()
    }
    catch {
      case e: Exception =>
        if (executorErrors.failureCauses == FailureCauses.NONE) {
          // If already error happened before task completion,
          // that error need to be thrown. Not the new error. Hence skip this.
          throw e
        }
    }
    finally {
      CommonUtil.clearUnsafeMemory(ThreadLocalTaskInfo.getCarbonTaskInfo.getTaskId)
      DataTypeUtil.clearFormatter()
    }
  }
}
