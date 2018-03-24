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

package org.apache.spark.sql.profiler

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerTaskEnd, SparkListenerTaskGettingResult, SparkListenerTaskStart}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

/**
 * listen sql execution
 */
private[profiler] class ProfilerListener extends SparkListener {
  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    Profiler.invokeIfEnable {
      event match {
        case executionStart: SparkListenerSQLExecutionStart =>
          Profiler.addExecutionMessage(
            executionStart.executionId,
            ExecutionStart(
              executionStart.executionId,
              executionStart.time,
              executionStart.physicalPlanDescription
            ))
        case executionEnd: SparkListenerSQLExecutionEnd =>
          Profiler.send(
            ExecutionEnd(
              executionEnd.executionId,
              executionEnd.time
            )
          )
        case _ =>
      }
    }
  }
}
