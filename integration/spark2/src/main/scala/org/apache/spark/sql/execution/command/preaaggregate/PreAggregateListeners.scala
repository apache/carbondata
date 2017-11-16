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
    if (table.hasDataMapSchema) {
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
    if (carbonTable.hasDataMapSchema) {
      val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList
      dataMapSchemas.asScala.foreach { dataMapSchema =>
        val childColumns = dataMapSchema.getChildSchema.getListOfColumns
        val parentColumnNames = childColumns.asScala
          .flatMap(_.getParentColumnTableRelations.asScala.map(_.getColumnName))
        if (parentColumnNames.contains(columnToBeAltered)) {
          throw new UnsupportedOperationException(
            s"Column $columnToBeAltered exists in a pre-aggregate table. Drop pre-aggregate table" +
            "to continue")
        }
      }
    }
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        s"Cannot change data type for columns in pre-aggregate table ${ carbonTable.getDatabaseName
        }.${ carbonTable.getTableName }")
    }
  }
}

object PreAggregateAddColumnsPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableAddColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        s"Cannot add columns in pre-aggreagate table ${ carbonTable.getDatabaseName
        }.${ carbonTable.getTableName }")
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
      if (carbonTable.hasDataMapSchema) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table. " +
          "Drop pre-aggregation table to continue")
      }
      if (carbonTable.isChildDataMap) {
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
      if (carbonTable.hasDataMapSchema) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables which have a pre-aggregate table")
      }
      if (carbonTable.isChildDataMap) {
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
    if (carbonTable.hasDataMapSchema) {
      val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList
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
            s"pre-aggregate table ${ dataMapSchema.getRelationIdentifier.toString }")
        }
      }
    }
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(s"Cannot drop columns in pre-aggreagate table ${
        carbonTable.getDatabaseName}.${ carbonTable.getTableName }")
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
    if (carbonTable.isChildDataMap) {
      throw new UnsupportedOperationException(
        "Rename operation for pre-aggregate table is not supported.")
    }
    if (carbonTable.hasDataMapSchema) {
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
      if (carbonTable.hasDataMapSchema) {
        throw new UnsupportedOperationException(
          "Update operation is not supported for tables which have a pre-aggregate table. Drop " +
          "pre-aggregate tables to continue.")
      }
      if (carbonTable.isChildDataMap) {
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
      if (carbonTable.hasDataMapSchema) {
        throw new UnsupportedOperationException(
          "Delete operation is not supported for tables which have a pre-aggregate table. Drop " +
          "pre-aggregate tables to continue.")
      }
      if (carbonTable.isChildDataMap) {
        throw new UnsupportedOperationException(
          "Delete operation is not supported for pre-aggregate table")
      }
    }
  }
}
