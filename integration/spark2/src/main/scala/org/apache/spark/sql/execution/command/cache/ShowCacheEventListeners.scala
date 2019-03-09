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

package org.apache.spark.sql.execution.command.cache

import java.util
import java.util.{HashSet, Set}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.CarbonEnv

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.datamap.bloom.{BloomCacheKeyValue, BloomCoarseGrainDataMapFactory}
import org.apache.carbondata.events._
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

object ShowCachePreAggEventListener extends OperationEventListener {

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
        val sparkSession = showTableCacheEvent.sparkSession
        val internalCall = showTableCacheEvent.internalCall
        if (carbonTable.isChildDataMap && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }

        val currentTableSizeMap = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[mutable.Map[String, (String, Long, Long)]]

        if (carbonTable.hasDataMapSchema) {
          val childrenSchemas = carbonTable.getTableInfo.getDataMapSchemaList.asScala
            .filter(_.getRelationIdentifier != null)
          for (childSchema <- childrenSchemas) {
            val datamapName = childSchema.getDataMapName
            val datamapProvider = childSchema.getProviderName
            val childCarbonTable = CarbonEnv.getCarbonTable(
              TableIdentifier(childSchema.getRelationIdentifier.getTableName,
                Some(carbonTable.getDatabaseName)))(sparkSession)

            val resultForChild = CarbonShowCacheCommand(None, true)
              .getTableCache(sparkSession, childCarbonTable)
            val datamapSize = resultForChild.head.getLong(1)
            currentTableSizeMap.put(datamapName, (datamapProvider, datamapSize, 0L))
          }
        }
    }
  }
}


object ShowCacheBloomEventListener extends OperationEventListener {

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
        val cache = CacheProvider.getInstance().getCarbonCache
        val currentTableSizeMap = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[mutable.Map[String, (String, Long, Long)]]

        // Extract all datamaps for the table
        val datamaps = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
          .asScala

        datamaps.foreach {
          case datamap if datamap.getProviderName
            .equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName) =>

            // Get datamap keys
            val datamapKeys = CacheUtil.getBloomCacheKeys(carbonTable, datamap)

            // calculate the memory size if key exists in cache
            val datamapSize = datamapKeys.collect {
              case key if cache.get(key) != null =>
                cache.get(key).getMemorySize
            }.sum

            // put the datmap size into main table's map.
            currentTableSizeMap
              .put(datamap.getDataMapName, (datamap.getProviderName, 0L, datamapSize))

          case _ =>
        }
    }
  }
}
