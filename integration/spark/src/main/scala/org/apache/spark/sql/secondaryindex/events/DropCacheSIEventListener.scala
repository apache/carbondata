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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.cache.CarbonDropCacheCommand
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{DropTableCacheEvent, Event, OperationContext, OperationEventListener}

object DropCacheSIEventListener extends OperationEventListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case dropCacheEvent: DropTableCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val sparkSession = dropCacheEvent.sparkSession
        val internalCall = dropCacheEvent.internalCall
        if (carbonTable.isIndexTable && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }

        val allIndexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
        val dbName = carbonTable.getDatabaseName
        for (indexTableName <- allIndexTables.asScala) {
          try {
            val dropCacheCommandForChildTable =
              CarbonDropCacheCommand(
                TableIdentifier(indexTableName, Some(dbName)),
                internalCall = true)
            dropCacheCommandForChildTable.processMetadata(sparkSession)
          }
          catch {
            case e: Exception =>
              LOGGER.error(s"Clean cache for SI table $indexTableName failed. ", e)
          }
        }

    }
  }
}
