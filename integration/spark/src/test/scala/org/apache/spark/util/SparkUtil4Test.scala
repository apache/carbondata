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

import mockit.{Invocation, Mock, MockUp}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, TaskContextImpl}

/**
 * This class is for accessing utils in spark package for tests
 */
object SparkUtil4Test {

  private var initializedMock = false

  def getConfiguredLocalDirs(conf: SparkConf): Array[String] = {
    Utils.getConfiguredLocalDirs(conf)
  }

  def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    Utils.getOrCreateLocalRootDirs(conf)
  }

  /**
   * Creates the mock for TaskContextImpl to catch the exception and ignore it for CI.
   * @param sqlContext
   */
  def createTaskMockUp(sqlContext: SQLContext): Unit = {
    if (!initializedMock) {
      if (sqlContext.sparkContext.version.startsWith("2.1")) {
        createTaskMockUp2_1
      } else if (sqlContext.sparkContext.version.startsWith("2.2")) {
        createTaskMockUp2_2()
      }
      initializedMock = true
    }
  }

  private def createTaskMockUp2_1 = {
    new MockUp[TaskContextImpl] {
      @Mock private[spark] def markTaskCompleted(invocation: Invocation): Unit = {
        try {
          invocation.proceed()
        } catch {
          case e: Exception => //ignore
        }
      }

      @Mock def addTaskCompletionListener(invocation: Invocation, listener: TaskCompletionListener): TaskContextImpl = {
        try {
          invocation.proceed(listener)
        } catch {
          case e: Exception => // ignore
          invocation.getInvokedInstance[TaskContextImpl]
        }
      }
    }
  }

  private def createTaskMockUp2_2(): Unit = {
    new MockUp[TaskContextImpl] {
      @Mock private[spark] def markTaskCompleted(invocation: Invocation, error: Option[Throwable]): Unit = {
        try {
          invocation.proceed(error)
        } catch {
          case e: Exception => //ignore
        }
      }
    }
  }
}
