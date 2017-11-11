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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AlterTableRenameModel
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil

import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.events.{AlterTableRenamePostEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.format.SchemaEvolutionEntry

object PreAggregateRenameTablePostListener extends OperationEventListener {

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val renameTablePostListener = event.asInstanceOf[AlterTableRenamePostEvent]
    val carbonTable = renameTablePostListener.carbonTable
    implicit val sparkSession: SparkSession = renameTablePostListener.sparkSession
    val renameTableModel = renameTablePostListener.alterTableRenameModel
    val oldParentTableName = renameTableModel.oldTableIdentifier.table
    val oldParentDatabaseName = renameTableModel.oldTableIdentifier.database.getOrElse("default")
    val newParentTableName = renameTableModel.newTableIdentifier.table
    if (carbonTable.hasPreAggregateTables) {
      val dataMapSchemas = carbonTable.getTableInfo.getDataMapSchemaList.asScala
      dataMapSchemas.foreach {
        dataMapSchema =>
          val childTableIdentifier = dataMapSchema.getRelationIdentifier
          val childCarbonTable = PreAggregateUtil
            .getChildCarbonTable(childTableIdentifier.getDatabaseName,
              childTableIdentifier.getTableName)(sparkSession)
          updateChildTableWithNewParent(renameTableModel, childCarbonTable)(sparkSession)
      }
    }
    val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
    schemaEvolutionEntry.setTime_stamp(System.currentTimeMillis())
    updateParentRelationIdentifierForColumns(carbonTable.getTableInfo,
      oldParentDatabaseName,
      oldParentTableName,
      newParentTableName)
    updateTableSchema(carbonTable,
      schemaEvolutionEntry,
      oldParentDatabaseName,
      oldParentTableName)
  }

  private def updateChildTableWithNewParent(renameTableModel: AlterTableRenameModel,
      childCarbonTable: Option[CarbonTable])(implicit sparkSession: SparkSession): Unit = {
    val oldParentTableName = renameTableModel.oldTableIdentifier.table
    val oldParentDatabaseName = renameTableModel.oldTableIdentifier.database.getOrElse("default")
    val newParentTableName = renameTableModel.newTableIdentifier.table
    if (childCarbonTable.isDefined) {
      val schemaEvolutionEntry = new SchemaEvolutionEntry(System.currentTimeMillis)
      schemaEvolutionEntry.setTime_stamp(System.currentTimeMillis())
      val childTableInfo = childCarbonTable.get.getTableInfo
      childTableInfo.getParentRelationIdentifiers.asScala.foreach {
        parentRelationIdentifier =>
          if (parentRelationIdentifier.getDatabaseName
                .equalsIgnoreCase(oldParentDatabaseName) &&
              parentRelationIdentifier.getTableName
                .equalsIgnoreCase(oldParentTableName)) {
            parentRelationIdentifier.setTableName(newParentTableName)
          }
      }
      updateTableSchema(childCarbonTable.get,
        schemaEvolutionEntry,
        childTableInfo.getDatabaseName,
        childTableInfo.getFactTable.getTableName)
    }
  }

  private def updateParentRelationIdentifierForColumns(parentTableInfo: TableInfo,
      oldParentDatabaseName: String,
      oldParentTableName: String,
      newParentTableName: String): Unit = {
    parentTableInfo.getFactTable.getListOfColumns.asScala.foreach {
      columnSchema =>
        columnSchema.getParentColumnTableRelations.asScala.foreach {
          parentColumnRelationIdentifier =>
            if (parentColumnRelationIdentifier.getRelationIdentifier.getDatabaseName
                  .equalsIgnoreCase(oldParentDatabaseName) &&
                parentColumnRelationIdentifier.getRelationIdentifier.getTableName
                  .equalsIgnoreCase(oldParentTableName)) {
              parentColumnRelationIdentifier.getRelationIdentifier
                .setTableName(newParentTableName)
            }
        }
    }
  }

  private def updateTableSchema(carbonTable: CarbonTable,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      oldDatabaseName: String, oldTableName: String)(implicit sparkSession: SparkSession): Unit = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val schemaConverter = new ThriftWrapperSchemaConverterImpl
      metaStore.updateTableSchemaForAlter(carbonTable.getCarbonTableIdentifier,
        carbonTable.getCarbonTableIdentifier,
        schemaConverter
          .fromWrapperToExternalTableInfo(carbonTable.getTableInfo,
            carbonTable.getDatabaseName,
            carbonTable.getFactTableName),
        schemaEvolutionEntry,
        carbonTable.getAbsoluteTableIdentifier.getTablePath)(sparkSession)
      sparkSession.catalog.refreshTable(TableIdentifier(carbonTable.getFactTableName,
        Some(carbonTable.getDatabaseName)).quotedString)
      metaStore.removeTableFromMetadata(oldDatabaseName, oldTableName)
    }

}
