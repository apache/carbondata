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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{DropCacheEvent, Event, OperationContext,
  OperationEventListener}

object DropCachePreAggEventListener extends OperationEventListener {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override protected def onEvent(event: Event,
      operationContext: OperationContext): Unit = {

    event match {
      case dropCacheEvent: DropCacheEvent =>
        val carbonTable = dropCacheEvent.carbonTable
        val sparkSession = dropCacheEvent.sparkSession

        if (carbonTable.hasDataMapSchema) {
          val childrenSchemas = carbonTable.getTableInfo.getDataMapSchemaList.asScala
            .filter(_.getRelationIdentifier != null)
          for (childSchema <- childrenSchemas) {
            val childTable =
              CarbonEnv.getCarbonTable(
                TableIdentifier(childSchema.getRelationIdentifier.getTableName,
                  Some(childSchema.getRelationIdentifier.getDatabaseName)))(sparkSession)
            val dropCacheCommandForChildTable = CarbonDropCacheCommand(TableIdentifier(childTable
              .getTableName, Some(childTable.getDatabaseName)), true)
            dropCacheCommandForChildTable.processMetadata(sparkSession)
          }
        }
    }

  }
}
