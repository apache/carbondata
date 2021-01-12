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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, CarbonThreadUtil, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.view.{MVCatalog, MVCatalogFactory, MVManager, MVSchema, MVStatus}

class MVManagerInSpark(session: SparkSession) extends MVManager {
  override def getDatabases: util.List[String] = {
    CarbonThreadUtil.threadSet(CarbonCommonConstants.CARBON_ENABLE_MV, "true")
    try {
      session.sessionState.catalog.listDatabases().asJava
    } finally {
      CarbonThreadUtil.threadUnset(CarbonCommonConstants.CARBON_ENABLE_MV)
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

  def disableMVOnTable(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      isOverwriteTable: Boolean = false): Unit = {
    if (carbonTable == null) {
      return
    }
    val viewManager = MVManagerInSpark.get(sparkSession)
    val viewSchemas = new util.ArrayList[MVSchema]()
    if (!viewManager.hasSchemaOnTable(carbonTable)) {
      return
    }
    for (viewSchema <- viewManager.getSchemasOnTable(carbonTable).asScala) {
      if (viewSchema.isRefreshOnManual) {
        viewSchemas.add(viewSchema)
      }
    }
    viewManager.setStatus(viewSchemas, MVStatus.DISABLED)
    if (isOverwriteTable) {
      if (!viewSchemas.isEmpty) {
        viewManager.onTruncate(viewSchemas)
      }
    }
  }

  /**
   * when first time MVCatalogs are initialized, it stores session info also,
   * but when carbon session is newly created, catalog map will not be cleared,
   * so if session info is different, remove the entry from map.
   */
  def getOrReloadMVCatalog(sparkSession: SparkSession): MVCatalogInSpark = {
    val catalogFactory = new MVCatalogFactory[MVSchemaWrapper] {
      override def newCatalog(): MVCatalog[MVSchemaWrapper] = {
        new MVCatalogInSpark(sparkSession)
      }
    }
    val viewManager = MVManagerInSpark.get(sparkSession)
    var viewCatalog = viewManager.getCatalog(catalogFactory, false).asInstanceOf[MVCatalogInSpark]
    if (!viewCatalog.session.equals(sparkSession)) {
      viewCatalog = viewManager.getCatalog(catalogFactory, true).asInstanceOf[MVCatalogInSpark]
    }
    viewCatalog
  }
}
