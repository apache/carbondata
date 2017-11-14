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
package org.apache.spark.sql.execution.command.datamap

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.command.CarbonDropTableCommand

import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.events.{DropDataMapPostEvent, DropTablePostEvent, Event, OperationContext, OperationEventListener}

object DataMapDropTablePostListener extends OperationEventListener {

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropPostEvent = event.asInstanceOf[DropTablePostEvent]
    val carbonTable = dropPostEvent.carbonTable
    val sparkSession = dropPostEvent.sparkSession
    if (carbonTable.isDefined && carbonTable.get.hasDataMapSchema) {
      val childSchemas = carbonTable.get.getTableInfo.getDataMapSchemaList
      for (childSchema: DataMapSchema <- childSchemas.asScala) {
        if (childSchema.getRelationIdentifier != null) {
          CarbonDropTableCommand(
            ifExistsSet = true,
            Some(childSchema.getRelationIdentifier.getDatabaseName),
            childSchema.getRelationIdentifier.getTableName, true
          ).run(sparkSession)
        }
      }
    }

  }
}

object DropDataMapPostListener extends OperationEventListener {

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropPostEvent = event.asInstanceOf[DropDataMapPostEvent]
    val dataMapSchema = dropPostEvent.dataMapSchema
    val sparkSession = dropPostEvent.sparkSession
    if (dataMapSchema.isDefined) {
      if (dataMapSchema.get.getRelationIdentifier != null) {
        CarbonDropTableCommand(ifExistsSet = true,
          Some(dataMapSchema.get.getRelationIdentifier.getDatabaseName),
          dataMapSchema.get.getRelationIdentifier.getTableName, true).run(sparkSession)
      }
    }
  }
}
