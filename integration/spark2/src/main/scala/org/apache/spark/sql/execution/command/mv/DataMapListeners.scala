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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.AlterTableModel
import org.apache.spark.sql.execution.command.management.CarbonAlterTableCompactionCommand

import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.events.{
  AlterTableCompactionPreStatusUpdateEvent,
  DeleteFromTablePostEvent, Event, OperationContext, OperationEventListener, UpdateTablePostEvent
}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostExecutionEvent
import org.apache.carbondata.processing.merger.CompactionType

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
