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

import org.apache.spark.sql.{CarbonUtils, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.view.MaterializedViewManager

class MaterializedViewManagerInSpark(session: SparkSession) extends MaterializedViewManager {
  override def getDatabases: util.List[String] = {
    CarbonUtils.threadSet(CarbonCommonConstants.DISABLE_SQL_REWRITE, "true")
    try {
      val databaseList = session.catalog.listDatabases()
      val databaseNameList = new util.ArrayList[String]()
      for (database <- databaseList.collect()) {
        databaseNameList.add(database.name)
      }
      databaseNameList
    } finally {
      CarbonUtils.threadUnset(CarbonCommonConstants.DISABLE_SQL_REWRITE)
    }
  }
}

object MaterializedViewManagerInSpark {

  private val MANAGER_MAP_BY_SESSION =
    new util.HashMap[SparkSession, MaterializedViewManagerInSpark]()

  def get(session: SparkSession): MaterializedViewManagerInSpark = {
    var viewManager = MANAGER_MAP_BY_SESSION.get(session)
    if (viewManager == null) {
      MANAGER_MAP_BY_SESSION.synchronized {
        viewManager = MANAGER_MAP_BY_SESSION.get(session)
        if (viewManager == null) {
          viewManager = new MaterializedViewManagerInSpark(session)
          MANAGER_MAP_BY_SESSION.put(session, viewManager)
        }
      }
    }
    viewManager
  }

}
