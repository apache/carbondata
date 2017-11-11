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

package org.apache.spark.sql.execution.command.preaaggregate.listeners

import scala.collection.JavaConverters._

import org.apache.carbondata.events.{AlterTableDataTypeChangePreEvent, Event, OperationContext, OperationEventListener}

object PreAggregateDataTypeChangePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableDataTypeChangePreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    val alterTableDataTypeChangeModel = dataTypeChangePreListener.alterTableDataTypeChangeModel
    val columnToBeAltered: String = alterTableDataTypeChangeModel.columnName
    val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList
    if (dataMapSchemas != null && !dataMapSchemas.isEmpty) {
      dataMapSchemas.asScala.foreach {
        dataMapSchema =>
          val childColumns = dataMapSchema.getChildSchema.getListOfColumns
          if (childColumns.asScala.exists(_.getColumnName == columnToBeAltered)) {
            throw new UnsupportedOperationException(s"Column $columnToBeAltered exists in a " +
                                                    s"pre-aggregate table ${
              dataMapSchema.toString}. Cannot change datatype")
          }
      }

      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(s"Cannot change data type for columns in " +
                                                s"pre-aggreagate table ${
          carbonTable.getDatabaseName}.${ carbonTable.getFactTableName }")
      }
    }
  }
}
