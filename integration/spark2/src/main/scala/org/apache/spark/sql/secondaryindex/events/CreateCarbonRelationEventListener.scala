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

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{CreateCarbonRelationPostEvent, Event, OperationContext, OperationEventListener}

class CreateCarbonRelationEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case createCarbonRelationPostEvent: CreateCarbonRelationPostEvent =>
        LOGGER.debug("Create carbon relation post event listener called")
        val carbonTable = createCarbonRelationPostEvent.carbonTable
        val databaseName = createCarbonRelationPostEvent.carbonTable.getDatabaseName
        val tableName = createCarbonRelationPostEvent.carbonTable.getTableName
        val sparkSession = createCarbonRelationPostEvent.sparkSession
        CarbonInternalMetastore
          .refreshIndexInfo(databaseName,
            tableName,
            carbonTable,
            createCarbonRelationPostEvent.needLock)(sparkSession)
    }
  }
}
