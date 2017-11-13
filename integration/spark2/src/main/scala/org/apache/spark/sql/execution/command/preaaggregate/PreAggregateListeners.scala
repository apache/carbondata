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
import org.apache.carbondata.events._

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
      dataMapSchemas.asScala.foreach { dataMapSchema =>
          val childColumns = dataMapSchema.getChildSchema.getListOfColumns
          if (childColumns.asScala.exists(_.getColumnName.equalsIgnoreCase(columnToBeAltered))) {
            throw new UnsupportedOperationException(s"Column $columnToBeAltered exists in a " +
                                                    s"pre-aggregate table ${ dataMapSchema.toString
                                                    }. Cannot change datatype")
          }
      }

      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(s"Cannot change data type for columns in " +
                                                s"pre-aggreagate table ${
                                                  carbonTable.getDatabaseName
                                                }.${ carbonTable.getFactTableName }")
      }
    }
  }
}

object PreAggregateDeleteSegmentByDatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val deleteSegmentByDatePreEvent = event.asInstanceOf[DeleteSegmentByDatePreEvent]
    val carbonTable = deleteSegmentByDatePreEvent.carbonTable
    if (carbonTable != null) {
      if (carbonTable.hasPreAggregateTables) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table. " +
          "Drop pre-aggregation table to continue")
      }
      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on pre-aggregate table")
      }
    }
  }
}

object PreAggregateDeleteSegmentByIdPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[DeleteSegmentByIdPreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (carbonTable.hasPreAggregateTables) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table")
      }
      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on pre-aggregate table")
      }
    }
  }

}

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
      dataMapSchemas.asScala.foreach { dataMapSchema =>
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

object PreAggregateRenameTablePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
    val renameTablePostListener = event.asInstanceOf[AlterTableRenamePreEvent]
    val carbonTable = renameTablePostListener.carbonTable
    if (carbonTable.isPreAggregateTable) {
      throw new UnsupportedOperationException(
        "Rename operation for pre-aggregate table is not supported.")
    }
    if (carbonTable.hasPreAggregateTables) {
      throw new UnsupportedOperationException(
        "Rename operation is not supported for table with pre-aggregate tables")
    }
  }
}

object UpdatePreAggregatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[UpdateTablePreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (carbonTable.hasPreAggregateTables) {
        throw new UnsupportedOperationException(
          "Update operation is not supported for tables which have a pre-aggregate table. Drop " +
          "pre-aggregate tables to continue.")
      }
      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(
          "Update operation is not supported for pre-aggregate table")
      }
    }
  }
}

object DeletePreAggregatePreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val tableEvent = event.asInstanceOf[DeleteFromTablePreEvent]
    val carbonTable = tableEvent.carbonTable
    if (carbonTable != null) {
      if (carbonTable.hasPreAggregateTables) {
        throw new UnsupportedOperationException(
          "Delete operation is not supported for tables which have a pre-aggregate table. Drop " +
          "pre-aggregate tables to continue.")
      }
      if (carbonTable.isPreAggregateTable) {
        throw new UnsupportedOperationException(
          "Delete operation is not supported for pre-aggregate table")
      }
    }
  }
}
