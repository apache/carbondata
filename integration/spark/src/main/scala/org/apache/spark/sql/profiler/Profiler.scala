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

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.util._

import org.apache.carbondata.common.annotations.{InterfaceAudience, InterfaceStability}
import org.apache.carbondata.core.util.CarbonProperties

/**
 * profiler end point util object
 */
@InterfaceAudience.Developer(Array("profiler"))
@InterfaceStability.Evolving
object Profiler {
  // whether it is enable or not
  private var isEnable = CarbonProperties.getInstance().isEnableQueryStatistics

  private var notInitialized = true

  // map statementId -> ProfilerMessage[]
  private lazy val statementMap = new util.HashMap[Long, ArrayBuffer[ProfilerMessage]]()

  // map executionId -> ProfilerMessage[]
  private lazy val executionMap = new util.HashMap[Long, ArrayBuffer[ProfilerMessage]]()

  private val endpointName = "CarbonProfiler"

  // setup EndpointRef to driver
  private lazy val setupEndpointRef =
    RpcUtils.makeDriverRef(endpointName, SparkEnv.get.conf, SparkEnv.get.rpcEnv)

  /**
   * setup profiler end point and register CarbonProfilerListener
   */
  def initialize(sparkContext: SparkContext): Unit = this.synchronized {
    invokeIfEnable {
      if (notInitialized) {
        notInitialized = false
        SparkEnv.get.rpcEnv.setupEndpoint(endpointName, new ProfilerEndPoint())
        sparkContext.addSparkListener(new ProfilerListener)
      }
    }
  }

  /**
   * run body if ProfilerEndPoint is enabled
   */
  def invokeIfEnable(body: => Unit): Unit = {
    if (isEnable) {
      body
    }
  }

  /**
   * send message to driver
   */
  def send(message: ProfilerMessage): Unit = {
    Profiler.setupEndpointRef.send(message)
  }

  /**
   * add message to statementMap
   */
  def addStatementMessage(statementId: Long, message: ProfilerMessage): Unit = this.synchronized {
    val profilerMessages = statementMap.get(statementId)
    if (profilerMessages == null) {
      statementMap.put(statementId, ArrayBuffer[ProfilerMessage](message))
    } else {
      profilerMessages += message
    }
  }

  /**
   * remove all messages of a statement by id
   */
  def removeStatementMessage(statementId: Long): ArrayBuffer[ProfilerMessage] = {
    statementMap.remove(statementId)
  }

  /**
   * add message to executionMap
   */
  def addExecutionMessage(executionId: Long, message: ProfilerMessage): Unit = this.synchronized {
    val profilerMessages = executionMap.get(executionId)
    if (profilerMessages == null) {
      executionMap.put(executionId, ArrayBuffer[ProfilerMessage](message))
    } else {
      profilerMessages += message
    }
  }

  /**
   * remove all messages of a execution by id
   */
  def removeExecutionMessage(executionId: Long): ArrayBuffer[ProfilerMessage] = {
    executionMap.remove(executionId)
  }

  def setIsEnable(isEnable: Boolean): Unit = {
    Profiler.isEnable = isEnable
  }
}

class ProfilerEndPoint extends RpcEndpoint {
  override val rpcEnv = SparkEnv.get.rpcEnv

  def processSQLStart(statementId: Long, messages: ArrayBuffer[ProfilerMessage]): Unit = {
    ProfilerLogger.logStatementSummary(statementId, messages)
  }

  def processExecutionEnd(executionId: Long, messages: ArrayBuffer[ProfilerMessage]): Unit = {
    ProfilerLogger.logExecutionSummary(executionId, messages)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case sqlStart: SQLStart =>
      if (sqlStart.isCommand) {
        // for the command sql, print summary to log file
        var messages = Profiler.removeStatementMessage(sqlStart.statementId)
        if (messages != null) {
          messages += sqlStart
        } else {
          messages = ArrayBuffer[ProfilerMessage](sqlStart)
        }
        processSQLStart(sqlStart.statementId, messages)
      }
    case optimizer: Optimizer =>
      val messages = Profiler.removeStatementMessage(optimizer.statementId)
      if (messages == null) {
        // the statement is a command, just add it to statementMap
        Profiler.addStatementMessage(optimizer.statementId, optimizer)
      } else {
        // this statement is a select query, print summary to log file
        messages += optimizer
        processSQLStart(optimizer.statementId, messages)
      }
    case getPartition: GetPartition =>
      Profiler.addExecutionMessage(getPartition.executionId, getPartition)
    case task: QueryTaskEnd =>
      Profiler.addExecutionMessage(task.executionId, task)
    case executionEnd: ExecutionEnd =>
      // print execution summary to log file
      val messages = Profiler.removeExecutionMessage(executionEnd.executionId)
      if (messages != null) {
        messages += executionEnd
        processExecutionEnd(executionEnd.executionId, messages)
      }
  }
}

/**
 * the trait of profiler messages
 */
trait ProfilerMessage

case class SQLStart(
    sqlText: String,
    statementId: Long,
    var startTime: Long = -1,
    var parseEnd: Long = -1,
    var analyzerEnd: Long = -1,
    var endTime: Long = -1,
    var isCommand: Boolean = false
) extends ProfilerMessage

case class Optimizer(
    statementId: Long,
    startTime: Long,
    timeTaken: Long
) extends ProfilerMessage

case class ExecutionStart(
    executionId: Long,
    startTime: Long,
    plan: String
) extends ProfilerMessage

case class ExecutionEnd(
    executionId: Long,
    endTime: Long
) extends ProfilerMessage

case class GetPartition(
    executionId: Long,
    tableName: String,
    tablePath: String,
    queryId: String,
    numOfPartitions: Int,
    startTime: Long,
    endTime: Long,
    getSplitsStart: Long,
    getSplitsEnd: Long,
    numSegments: Int,
    numStreamSegments: Int,
    numBlocks: Int,
    distributeStart: Long,
    distributeEnd: Long,
    filter: String,
    projection: String
) extends ProfilerMessage with Comparable[GetPartition] {
  override def compareTo(other: GetPartition): Int = {
    queryId.compareTo(other.queryId)
  }
}

case class QueryTaskEnd(
    executionId: Long,
    queryId: String,
    values: Array[Long],
    size: Long,
    files: Array[String]
) extends ProfilerMessage with Comparable[QueryTaskEnd] {
  override def compareTo(other: QueryTaskEnd): Int = {
    val result = this.queryId.compareTo(other.queryId)
    if (result != 0) {
      result
    } else {
      val task = this.values(1) - other.values(1)
      if (task > 0) {
        1
      } else if (task < 0) {
        -1
      } else {
        0
      }
    }
  }
}
