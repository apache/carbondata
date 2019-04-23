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

package org.apache.spark.sql.execution.command.mv

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.AlterTableModel
import org.apache.spark.sql.execution.command.management.CarbonAlterTableCompactionCommand
import org.apache.spark.util.DataMapUtil

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostExecutionEvent
import org.apache.carbondata.processing.merger.CompactionType


object DataMapListeners {
  def getDataMapTableColumns(dataMapSchema: DataMapSchema,
      carbonTable: CarbonTable): mutable.Buffer[String] = {
    val listOfColumns: mutable.Buffer[String] = new mutable.ArrayBuffer[String]()
    listOfColumns.asJava
      .addAll(dataMapSchema.getMainTableColumnList.get(carbonTable.getTableName))
    listOfColumns
  }
}

/**
 * Listener to trigger compaction on mv datamap after main table compaction
 */
object AlterDataMaptableCompactionPostListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val compactionEvent = event.asInstanceOf[AlterTableCompactionPreStatusUpdateEvent]
    val carbonTable = compactionEvent.carbonTable
    val compactionType = compactionEvent.carbonMergerMapping.campactionType
    if (compactionType == CompactionType.CUSTOM) {
      return
    }
    val carbonLoadModel = compactionEvent.carbonLoadModel
    val sparkSession = compactionEvent.sparkSession
    val allDataMapSchemas = DataMapStoreManager.getInstance
      .getDataMapSchemasOfTable(carbonTable).asScala
      .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                               !dataMapSchema.isIndexDataMap)
    if (!allDataMapSchemas.asJava.isEmpty) {
      allDataMapSchemas.foreach { dataMapSchema =>
        val childRelationIdentifier = dataMapSchema.getRelationIdentifier
        val alterTableModel = AlterTableModel(Some(childRelationIdentifier.getDatabaseName),
          childRelationIdentifier.getTableName,
          None,
          compactionType.toString,
          Some(System.currentTimeMillis()),
          "")
        operationContext.setProperty(
          dataMapSchema.getRelationIdentifier.getDatabaseName + "_" +
          dataMapSchema.getRelationIdentifier.getTableName + "_Segment",
          carbonLoadModel.getSegmentId)
        CarbonAlterTableCompactionCommand(alterTableModel, operationContext = operationContext)
          .run(sparkSession)
      }
    }
  }
}

/**
 * Listener to trigger data load on mv datamap after main table data load
 */
object LoadPostDataMapListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val sparkSession = SparkSession.getActiveSession.get
    val carbonTable: CarbonTable =
      event match {
        case event: LoadTablePostExecutionEvent =>
          val carbonLoadModelOption = Some(event.getCarbonLoadModel)
          if (carbonLoadModelOption.isDefined) {
            val carbonLoadModel = carbonLoadModelOption.get
            carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
          } else {
            null
          }
        case event: UpdateTablePostEvent =>
          val table = Some(event.carbonTable)
          if (table.isDefined) {
            table.get
          } else {
            null
          }
        case event: DeleteFromTablePostEvent =>
          val table = Some(event.carbonTable)
          if (table.isDefined) {
            table.get
          } else {
            null
          }
        case _ => null
      }
    if (null != carbonTable) {
      val allDataMapSchemas = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
        .filter(dataMapSchema => null != dataMapSchema.getRelationIdentifier &&
                                 !dataMapSchema.isIndexDataMap)
      if (!allDataMapSchemas.asJava.isEmpty) {
        allDataMapSchemas.foreach { dataMapSchema =>
          if (!dataMapSchema.isLazy) {
            val provider = DataMapManager.get()
              .getDataMapProvider(carbonTable, dataMapSchema, sparkSession)
            try {
              provider.rebuild()
            } catch {
              case ex: Exception =>
                DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName)
                throw ex
            }
            DataMapStatusManager.enableDataMap(dataMapSchema.getDataMapName)
          }
        }
      }
    }
  }
}

/**
 * Listeners to block operations like delete segment on id or by date on tables
 * having an mv datamap or on mv datamap tables
 */
object DataMapDeleteSegmentPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val carbonTable = event match {
      case e: DeleteSegmentByIdPreEvent =>
        e.asInstanceOf[DeleteSegmentByIdPreEvent].carbonTable
      case e: DeleteSegmentByDatePreEvent =>
        e.asInstanceOf[DeleteSegmentByDatePreEvent].carbonTable
    }
    if (null != carbonTable) {
      if (DataMapUtil.hasMVDataMap(carbonTable)) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on tables having child datamap")
      }
      if (carbonTable.isChildTable) {
        throw new UnsupportedOperationException(
          "Delete segment operation is not supported on datamap table")
      }
    }
  }
}

object DataMapAddColumnsPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dataTypeChangePreListener = event.asInstanceOf[AlterTableAddColumnPreEvent]
    val carbonTable = dataTypeChangePreListener.carbonTable
    if (carbonTable.isChildTable) {
      throw new UnsupportedOperationException(
        s"Cannot add columns in a DataMap table " +
        s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    }
  }
}


object DataMapDropColumnPreListener extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val dropColumnChangePreListener = event.asInstanceOf[AlterTableDropColumnPreEvent]
    val carbonTable = dropColumnChangePreListener.carbonTable
    val alterTableDropColumnModel = dropColumnChangePreListener.alterTableDropColumnModel
    val columnsToBeDropped = alterTableDropColumnModel.columns
    if (DataMapUtil.hasMVDataMap(carbonTable)) {
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
      for (dataMapSchema <- dataMapSchemaList) {
        if (null != dataMapSchema && !dataMapSchema.isIndexDataMap) {
          val listOfColumns = DataMapListeners.getDataMapTableColumns(dataMapSchema, carbonTable)
          val columnExistsInChild = listOfColumns.collectFirst {
            case parentColumnName if columnsToBeDropped.contains(parentColumnName) =>
              parentColumnName
          }
          if (columnExistsInChild.isDefined) {
            throw new UnsupportedOperationException(
              s"Column ${ columnExistsInChild.head } cannot be dropped because it exists " +
              s"in " + dataMapSchema.getProviderName + " datamap:" +
              s"${ dataMapSchema.getDataMapName }")
          }
        }
      }
    }
    if (carbonTable.isChildTable) {
      throw new UnsupportedOperationException(
        s"Cannot drop columns present in a datamap table ${ carbonTable.getDatabaseName }." +
        s"${ carbonTable.getTableName }")
    }
  }
}

object DataMapChangeDataTypeorRenameColumnPreListener
  extends OperationEventListener {
  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val colRenameDataTypeChangePreListener = event
      .asInstanceOf[AlterTableColRenameAndDataTypeChangePreEvent]
    val carbonTable = colRenameDataTypeChangePreListener.carbonTable
    val alterTableDataTypeChangeModel = colRenameDataTypeChangePreListener
      .alterTableDataTypeChangeModel
    val columnToBeAltered: String = alterTableDataTypeChangeModel.columnName
    if (DataMapUtil.hasMVDataMap(carbonTable)) {
      val dataMapSchemaList = DataMapStoreManager.getInstance
        .getDataMapSchemasOfTable(carbonTable).asScala
      for (dataMapSchema <- dataMapSchemaList) {
        if (null != dataMapSchema && !dataMapSchema.isIndexDataMap) {
          val listOfColumns = DataMapListeners.getDataMapTableColumns(dataMapSchema, carbonTable)
          if (listOfColumns.contains(columnToBeAltered)) {
            throw new UnsupportedOperationException(
              s"Column $columnToBeAltered exists in a " + dataMapSchema.getProviderName +
              " datamap. Drop " + dataMapSchema.getProviderName + "  datamap to continue")
          }
        }
      }
    }
    if (carbonTable.isChildTable) {
      throw new UnsupportedOperationException(
        s"Cannot change data type or rename column for columns present in mv datamap table " +
        s"${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
    }
  }
}
