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
package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.hive.execution.command.CarbonSetCommand
import org.apache.spark.sql.profiler.{Profiler, SQLStart}

import org.apache.carbondata.core.util.{CarbonSessionInfo, ThreadLocalSessionInfo}

object CarbonUtils {

  private val statementId = new AtomicLong(0)

  private[sql] val threadStatementId = new ThreadLocal[Long]

  private def withProfiler(sparkSession: SparkSession,
                            sqlText: String,
                            generateDF: (QueryExecution, SQLStart) => DataFrame): DataFrame = {
    val sse = SQLStart(sqlText, CarbonUtils.statementId.getAndIncrement())
    CarbonUtils.threadStatementId.set(sse.statementId)
    sse.startTime = System.currentTimeMillis()

    try {
      val logicalPlan = sparkSession.sessionState.sqlParser.parsePlan(sqlText)
      sse.parseEnd = System.currentTimeMillis()

      val qe = sparkSession.sessionState.executePlan(logicalPlan)
      qe.assertAnalyzed()
      sse.isCommand = qe.analyzed match {
        case c: Command => true
        case u @ Union(children) if children.forall(_.isInstanceOf[Command]) => true
        case _ => false
      }
      sse.analyzerEnd = System.currentTimeMillis()
      generateDF(qe, sse)
    } finally {
      Profiler.invokeIfEnable {
        if (sse.isCommand) {
          sse.endTime = System.currentTimeMillis()
          Profiler.send(sse)
        } else {
          Profiler.addStatementMessage(sse.statementId, sse)
        }
      }
    }
  }

  def threadSet(key: String, value: String): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    }
    else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    val threadParams = currentThreadSessionInfo.getThreadParams
    CarbonSetCommand.validateAndSetValue(threadParams, key, value)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }


  def threadSet(key: String, value: Object): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    }
    else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    currentThreadSessionInfo.getThreadParams.setExtraInfo(key, value)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }

  def threadUnset(key: String): Unit = {
    val currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo != null) {
      val currentThreadSessionInfoClone = currentThreadSessionInfo.clone()
      val threadParams = currentThreadSessionInfoClone.getThreadParams
      CarbonSetCommand.unsetValue(threadParams, key)
      threadParams.removeExtraInfo(key)
      ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfoClone)
    }
  }

  def updateSessionInfoToCurrentThread(sparkSession: SparkSession): Unit = {
    val carbonSessionInfo = CarbonEnv.getInstance(sparkSession).carbonSessionInfo.clone()
    val currentThreadSessionInfoOrig = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfoOrig != null) {
      val currentThreadSessionInfo = currentThreadSessionInfoOrig.clone()
      // copy all the thread parameters to apply to session parameters
      currentThreadSessionInfo.getThreadParams.getAll.asScala
        .foreach(entry => carbonSessionInfo.getSessionParams.addProperty(entry._1, entry._2))
      carbonSessionInfo.setThreadParams(currentThreadSessionInfo.getThreadParams)
    }
    // preserve thread parameters across call
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
  }
}
