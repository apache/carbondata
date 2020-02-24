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
package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.sql.CarbonEnv

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener, ShowTableCacheEvent}

object ShowCacheSIEventListener extends OperationEventListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>
        val carbonTable = showTableCacheEvent.carbonTable
        val sparkSession = showTableCacheEvent.sparkSession
        val internalCall = showTableCacheEvent.internalCall
        if (carbonTable.isIndexTable && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on index table.")
        }

        val childTables = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[List[(String, String)]]

        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          operationContext.setProperty(carbonTable.getTableUniqueName, indexTables.map {
            indexTable =>
              val indexCarbonTable = CarbonEnv.getCarbonTable(Some(carbonTable.getDatabaseName),
                indexTable)(sparkSession)
              (carbonTable.getDatabaseName + "-" +
               indexTable, "Secondary Index", indexCarbonTable.getTableId)
          }.toList ++ childTables)
        }
    }
  }
}
