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
import scala.collection.mutable

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.cache.{CacheUtil, CarbonDropCacheCommand}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider
import org.apache.carbondata.core.metadata.schema.table.IndexSchema
import org.apache.carbondata.events.{DropTableCacheEvent, Event, OperationContext, OperationEventListener}

object DropCacheMVEventListener extends OperationEventListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropCacheEvent: DropTableCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val sparkSession = dropCacheEvent.sparkSession
        val internalCall = dropCacheEvent.internalCall
        if (carbonTable.isMV && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }

        if (carbonTable.hasMVCreated) {
          val childrenSchemas = IndexStoreManager.getInstance
            .getDataMapSchemasOfTable(carbonTable).asScala
            .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                     !dataMapSchema.isIndex)
          dropCacheForChildTables(sparkSession, childrenSchemas)
        }
    }
  }

  private def dropCacheForChildTables(sparkSession: SparkSession,
      childrenSchemas: mutable.Buffer[IndexSchema]): Unit = {
    for (childSchema <- childrenSchemas) {
      val childTable =
        CarbonEnv.getCarbonTable(
          TableIdentifier(childSchema.getRelationIdentifier.getTableName,
            Some(childSchema.getRelationIdentifier.getDatabaseName)))(sparkSession)
      try {
        val dropCacheCommandForChildTable =
          CarbonDropCacheCommand(
            TableIdentifier(childTable.getTableName, Some(childTable.getDatabaseName)),
            internalCall = true)
        dropCacheCommandForChildTable.processMetadata(sparkSession)
      }
      catch {
        case e: Exception =>
          LOGGER.warn(
            s"Clean cache for child table ${ childTable.getTableName } failed.", e)
      }
    }
  }
}


object DropCacheBloomEventListener extends OperationEventListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropCacheEvent: DropTableCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val cache = CacheProvider.getInstance().getCarbonCache
        val indexMetadata = carbonTable.getIndexMetadata
        val bloomIndexProvider = CarbonIndexProvider.BLOOMFILTER.getIndexProviderName
        if (null != indexMetadata && null != indexMetadata.getIndexesMap &&
            null != indexMetadata.getIndexesMap.get(bloomIndexProvider)) {
          val bloomIndexes = indexMetadata.getIndexesMap.get(bloomIndexProvider)
          val bloomIndexIterator = bloomIndexes.entrySet().iterator()
          while (bloomIndexIterator.hasNext) {
            val bloomIndexEntry = bloomIndexIterator.next()
            val index = new IndexSchema(bloomIndexEntry.getKey, bloomIndexProvider)
            index.setProperties(bloomIndexEntry.getValue)
            try {
              // Get index keys
              val indexKeys = CacheUtil.getBloomCacheKeys(carbonTable, index)

              // remove index keys from cache
              cache.removeAll(indexKeys.asJava)
            } catch {
              case e: Exception =>
                LOGGER.warn(
                  s"Clean cache for Bloom index ${ index.getIndexName } failed.", e)
            }
          }
        }
    }
  }
}
