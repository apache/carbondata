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

import org.apache.carbondata.events.{AlterTableDropColumnPreEvent, Event, OperationContext, OperationEventListener}

object PreAggregateDropColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    val alterTableDropColumnModel = dataTypeChangePreListener.alterTableDropColumnModel
    val columnsToBeDropped = alterTableDropColumnModel.columns
    val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList
    if (dataMapSchemas != null && !dataMapSchemas.isEmpty) {
      dataMapSchemas.asScala.foreach {
        dataMapSchema =>
          val parentColumnNames = dataMapSchema.getChildSchema.getListOfColumns.asScala
            .flatMap(_.getParentColumnTableRelations.asScala.map(_.getColumnName))
          val columnExistsInChild = parentColumnNames.collectFirst {
            case parentColumnName if columnsToBeDropped.contains(parentColumnName) =>
              parentColumnName
          }
          if (columnExistsInChild.isDefined) {
            throw new UnsupportedOperationException(
              s"Column ${ columnExistsInChild.head } cannot be dropped because it exists in a " +
              s"pre-aggregate table ${ dataMapSchema.getRelationIdentifier.toString}")
          }
      }
      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(s"Cannot drop columns in pre-aggreagate table ${
          carbonTable.getDatabaseName}.${ carbonTable.getFactTableName }")
      }
    }
  }
}
