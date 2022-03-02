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
   * when first time MVCatalogs are initialized, it stores with session info.
   * but when mv schemas are updated in other sessions or carbon session is newly created,
   * need to update the schemas of existing catalog or create new catalog and load entries.
   */
  def getMVCatalog(sparkSession: SparkSession): MVCatalogInSpark = {
    val catalogFactory = new MVCatalogFactory[MVSchemaWrapper] {
      override def newCatalog(): MVCatalog[MVSchemaWrapper] = {
        new MVCatalogInSpark(sparkSession)
      }
    }
    val viewManager = MVManagerInSpark.get(sparkSession)
    var currSchemas: util.List[MVSchema] = new util.ArrayList[MVSchema]()
    var viewCatalog = viewManager.getCatalog
    if (viewCatalog != null) {
      currSchemas = viewCatalog.asInstanceOf[MVCatalogInSpark]
        .getAllSchemas.toStream.map(_.viewSchema).toList.asJava
    }
    // update the schemas in catalog
    viewCatalog = viewManager.getCatalog(catalogFactory, currSchemas)
    viewCatalog.asInstanceOf[MVCatalogInSpark]
  }
}
