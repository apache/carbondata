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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.cache.{CacheUtil, CarbonDropCacheCommand}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.events.{DropTableCacheEvent, Event, OperationContext, OperationEventListener}

object DropCacheDataMapEventListener extends OperationEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropCacheEvent: DropTableCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val sparkSession = dropCacheEvent.sparkSession
        val internalCall = dropCacheEvent.internalCall
        if (carbonTable.isChildTableForMV && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }

        if (carbonTable.hasMVCreated) {
          val childrenSchemas = DataMapStoreManager.getInstance
            .getDataMapSchemasOfTable(carbonTable).asScala
            .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                     !dataMapSchema.isIndexDataMap)
          dropCacheForChildTables(sparkSession, childrenSchemas)
        }
    }
  }

  private def dropCacheForChildTables(sparkSession: SparkSession,
      childrenSchemas: mutable.Buffer[DataMapSchema]): Unit = {
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

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropCacheEvent: DropTableCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val cache = CacheProvider.getInstance().getCarbonCache
        val datamaps = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable)
          .asScala.toList
        datamaps.foreach {
          case datamap if datamap.getProviderName
            .equalsIgnoreCase(DataMapClassProvider.BLOOMFILTER.getShortName) =>
            try {
              // Get datamap keys
              val datamapKeys = CacheUtil.getBloomCacheKeys(carbonTable, datamap)

              // remove datamap keys from cache
              cache.removeAll(datamapKeys.asJava)
            } catch {
              case e: Exception =>
                LOGGER.warn(
                  s"Clean cache for Bloom datamap ${datamap.getDataMapName} failed.", e)
            }
          case _ =>
        }
    }
  }
}
