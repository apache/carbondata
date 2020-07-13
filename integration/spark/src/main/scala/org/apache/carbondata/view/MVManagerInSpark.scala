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

package org.apache.carbondata.view

import java.util

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{CarbonEnv, CarbonUtils, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.ThreadLocalSessionInfo
import org.apache.carbondata.core.view.MVManager

class MVManagerInSpark(session: SparkSession) extends MVManager {
  override def getDatabases: util.List[String] = {
    CarbonUtils.threadSet(CarbonCommonConstants.CARBON_ENABLE_MV, "true")
    try {
      val databaseList = session.catalog.listDatabases()
      val databaseNameList = new util.ArrayList[String]()
      for (database <- databaseList.collect()) {
        databaseNameList.add(database.name)
      }
      databaseNameList
    } finally {
      CarbonUtils.threadUnset(CarbonCommonConstants.CARBON_ENABLE_MV)
    }
  }

  override def getDatabaseLocation(databaseName: String): String = {
    CarbonEnv.getDatabaseLocation(databaseName, session)
  }
}

object MVManagerInSpark {

  private var viewManager: MVManagerInSpark = null

  // returns single MVManager instance for all the current sessions.
  def get(session: SparkSession): MVManagerInSpark = {
    if (viewManager == null) {
      this.synchronized {
        if (viewManager == null) {
          viewManager = new MVManagerInSpark(session)
        }
      }
    }
    viewManager
  }

}
