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

package org.apache.spark.sql.listeners

import scala.collection.JavaConverters._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.events._

object ShowCachePreMVEventListener extends OperationEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>
        val carbonTable = showTableCacheEvent.carbonTable
        val internalCall = showTableCacheEvent.internalCall
        if (carbonTable.isChildTableForMV && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }
    }
  }
}


object ShowCacheDataMapEventListener extends OperationEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>
        val carbonTable = showTableCacheEvent.carbonTable
        val childTables = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[List[(String, String)]]

        // Extract all datamaps for the table
        val datamaps = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
          .asScala.toList

        val bloomDataMaps = filterDataMaps(datamaps, DataMapClassProvider.BLOOMFILTER.getShortName)

        val mvDataMaps = filterDataMaps(datamaps, DataMapClassProvider.MV.getShortName)
        operationContext
          .setProperty(carbonTable.getTableUniqueName, childTables ++ bloomDataMaps ++ mvDataMaps)
    }
  }

  private def filterDataMaps(dataMaps: List[DataMapSchema],
      filter: String): List[(String, String, String)] = {
    dataMaps.collect {
      case dataMap if dataMap.getProviderName
        .equalsIgnoreCase(filter) =>
        if (filter.equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName)) {
          (s"${ dataMap.getRelationIdentifier.getDatabaseName }-${
            dataMap.getDataMapName}", dataMap.getProviderName,
            dataMap.getRelationIdentifier.getTableId)
        } else {
          (s"${ dataMap.getRelationIdentifier.getDatabaseName }-${
            dataMap.getRelationIdentifier.getTableName}", dataMap.getProviderName,
            dataMap.getRelationIdentifier.getTableId)
        }
    }
  }
}
