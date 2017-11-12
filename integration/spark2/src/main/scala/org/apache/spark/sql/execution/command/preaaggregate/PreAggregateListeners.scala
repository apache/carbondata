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

package org.apache.spark.sql.execution.command.preaaggregate

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonSession
import org.apache.spark.sql.execution.command.CarbonDropTableCommand

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema
import org.apache.carbondata.events.{DropTablePostEvent, Event, LoadTablePostExecutionEvent, OperationContext, OperationEventListener}

object DropPreAggregateTablePostListener extends OperationEventListener {

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropPostEvent = event.asInstanceOf[DropTablePostEvent]
    val carbonTable = dropPostEvent.carbonTable
    val sparkSession = dropPostEvent.sparkSession
    if (carbonTable.isDefined && carbonTable.get.getTableInfo.getDataMapSchemaList != null &&
        !carbonTable.get.getTableInfo.getDataMapSchemaList.isEmpty) {
      val childSchemas = carbonTable.get.getTableInfo.getDataMapSchemaList
      for (childSchema: DataMapSchema <- childSchemas.asScala) {
        CarbonDropTableCommand(ifExistsSet = true,
          Some(childSchema.getRelationIdentifier.getDatabaseName),
          childSchema.getRelationIdentifier.getTableName).run(sparkSession)
      }
    }

  }
}

object LoadPostAggregateListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val loadEvent = event.asInstanceOf[LoadTablePostExecutionEvent]
    val sparkSession = loadEvent.sparkSession
    val carbonLoadModel = loadEvent.carbonLoadModel
    val table = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    if (!table.getTableInfo.getDataMapSchemaList.isEmpty) {
      for (dataMapSchema: DataMapSchema <- table.getTableInfo.getDataMapSchemaList.asScala) {
        CarbonSession
          .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     carbonLoadModel.getDatabaseName + "." +
                     carbonLoadModel.getTableName,
            carbonLoadModel.getSegmentId)
        CarbonSession.threadSet(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                                carbonLoadModel.getDatabaseName + "." +
                                carbonLoadModel.getTableName, "false")
        val childTableName = dataMapSchema.getRelationIdentifier.getTableName
        val childDatabaseName = dataMapSchema.getRelationIdentifier.getDatabaseName
        val selectQuery = dataMapSchema.getProperties.get("CHILD_SELECT QUERY")
        sparkSession.sql(s"insert into $childDatabaseName.$childTableName $selectQuery")
      }
    }
  }
}
