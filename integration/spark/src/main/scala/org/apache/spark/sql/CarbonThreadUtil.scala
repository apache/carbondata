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

import scala.collection.JavaConverters._

import org.apache.spark.sql.hive.execution.command.CarbonSetCommand

import org.apache.carbondata.core.util.{CarbonSessionInfo, ThreadLocalSessionInfo}

object CarbonThreadUtil {

  private[sql] val threadStatementId = new ThreadLocal[Long]

  /**
   * set CarbonSessionInfo of ThreadLocal
   * @param f change CarbonSessionInfo
   */
  private def threadSet(f: CarbonSessionInfo => Unit): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    } else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    f(currentThreadSessionInfo)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }

  def threadSet(key: String, value: String): Unit = {
    threadSet { carbonSessionInfo =>
      CarbonSetCommand.validateAndSetValue(carbonSessionInfo.getThreadParams, key, value)
    }
  }

  def threadSet(key: String, value: Object): Unit = {
    threadSet(_.getThreadParams.setExtraInfo(key, value))
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
