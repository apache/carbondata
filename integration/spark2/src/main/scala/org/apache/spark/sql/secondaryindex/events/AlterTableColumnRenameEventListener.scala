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

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.execution.command.AlterTableDataTypeChangeModel
import org.apache.spark.sql.execution.command.schema.CarbonAlterTableColRenameDataTypeChangeCommand
import org.apache.spark.sql.hive.CarbonHiveMetadataUtil
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.indextable.IndexTableInfo
import org.apache.carbondata.events._
import org.apache.carbondata.events.exception.PostEventException
import org.apache.carbondata.format.TableInfo

/**
 * Listener class to rename the column present in index tables
 */
class AlterTableColumnRenameEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override protected def onEvent(event: Event,
    operationContext: OperationContext): Unit = {
    event match {
      case alterTableColRenameAndDataTypeChangePreEvent
        : AlterTableColRenameAndDataTypeChangePreEvent =>
        val carbonTable = alterTableColRenameAndDataTypeChangePreEvent.carbonTable
        // direct column rename on index table is not allowed
        if (carbonTable.isIndexTable) {
          if (!operationContext.getProperty("childTableColumnRename").toString.toBoolean) {
            throw new MalformedCarbonCommandException(
              "Alter table column rename is not allowed on index table.")
          }
        }
      case alterTableColRenameAndDataTypeChangePostEvent
        : AlterTableColRenameAndDataTypeChangePostEvent
        if alterTableColRenameAndDataTypeChangePostEvent
          .alterTableDataTypeChangeModel.isColumnRename =>
        val alterTableDataTypeChangeModel = alterTableColRenameAndDataTypeChangePostEvent
          .alterTableDataTypeChangeModel
        val sparkSession = alterTableColRenameAndDataTypeChangePostEvent.sparkSession
        val databaseName = alterTableDataTypeChangeModel.databaseName
        val carbonTable = alterTableColRenameAndDataTypeChangePostEvent.carbonTable
        val catalog = CarbonEnv
          .getInstance(alterTableColRenameAndDataTypeChangePostEvent.sparkSession).carbonMetaStore
        val newColumnName = alterTableDataTypeChangeModel.newColumnName
        val oldColumnName = alterTableDataTypeChangeModel.columnName
        val dataTypeInfo = alterTableDataTypeChangeModel.dataTypeInfo
        val carbonColumns = carbonTable
          .getCreateOrderColumn.asScala
          .filter(!_.isInvisible)
        val carbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(oldColumnName))
        var indexTablesToRenameColumn: Seq[String] = Seq.empty
        CarbonInternalScalaUtil.getIndexesMap(carbonTable).asScala.foreach(
          indexTable =>
            indexTable._2.asScala.foreach(column =>
              if (oldColumnName.equalsIgnoreCase(column)) {
                indexTablesToRenameColumn ++= Seq(indexTable._1)
              }))
        val indexTablesRenamedSuccess = indexTablesToRenameColumn
          .takeWhile { indexTable =>
            val alterTableColRenameAndDataTypeChangeModel =
              AlterTableDataTypeChangeModel(
                dataTypeInfo,
                databaseName,
                indexTable,
                oldColumnName,
                newColumnName,
                alterTableDataTypeChangeModel.isColumnRename
              )
            // Fire CarbonAlterTableColRenameDataTypeChangeCommand for each index tables
            try {
              CarbonAlterTableColRenameDataTypeChangeCommand(
                alterTableColRenameAndDataTypeChangeModel, childTableColumnRename = true)
                .run(alterTableColRenameAndDataTypeChangePostEvent.sparkSession)
              LOGGER
                .info(s"Column rename for index $indexTable is successful. Index column " +
                      s"$oldColumnName is successfully renamed to $newColumnName")
              true
            } catch {
              case ex: Exception =>
                LOGGER
                  .error(
                    "column rename is failed for index table, reverting the changes for all the " +
                    "successfully renamed index tables.",
                    ex)
                false
            }
          }
        // if number of successful index table column rename should be equal to total index tables
        // to rename column, else revert the successful ones
        val needRevert = indexTablesToRenameColumn.length != indexTablesRenamedSuccess.length
        if (needRevert) {
          indexTablesRenamedSuccess.foreach { indexTable =>
            val indexCarbonTable = CarbonEnv.getCarbonTable(databaseName, indexTable)(sparkSession)
            if (indexCarbonTable != null) {
              // failure tables will be automatically taken care in
              // CarbonAlterTableColRenameDataTypeChangeCommand, just need to revert the success
              // tables, so get the latest timestamp for evolutionhistory
              val thriftTable: TableInfo = catalog.getThriftTableInfo(indexCarbonTable)
              val evolutionEntryList = thriftTable.fact_table.schema_evolution
                .schema_evolution_history
              AlterTableUtil
                .revertColumnRenameAndDataTypeChanges(indexCarbonTable.getDatabaseName,
                  indexCarbonTable.getTableName,
                  evolutionEntryList.get(evolutionEntryList.size() - 1).time_stamp)(
                  alterTableColRenameAndDataTypeChangePostEvent.sparkSession)
            }
          }
          throw PostEventException("Alter table column rename failed for index tables")
        } else {
          val database = sparkSession.catalog.currentDatabase
          if (indexTablesRenamedSuccess.nonEmpty) {
            // set the new indexInfo after column rename
            val oldIndexInfo = carbonTable.getIndexInfo
            val indexInfo = IndexTableInfo
              .updateIndexColumns(oldIndexInfo, oldColumnName, newColumnName)
            sparkSession.sql(
              s"""ALTER TABLE $database.${
                carbonTable.getTableName
              } SET SERDEPROPERTIES ('indexInfo' = '$indexInfo')""".stripMargin)
            CarbonEnv.getInstance(sparkSession).carbonMetaStore
              .removeTableFromMetadata(carbonTable.getDatabaseName, carbonTable.getTableName)
          }
          CarbonHiveMetadataUtil.refreshTable(database, carbonTable.getTableName, sparkSession)
        }
      case _ =>
    }
  }
}
